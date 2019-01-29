/*
 * Copyright 2017-2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.traffic.generator;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.function.DateToTimeBucketEnd;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition;
import uk.gov.gchq.gaffer.data.element.function.TuplesToElements;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.HyperLogLogPlusElementGenerator;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.function.And;
import uk.gov.gchq.koryphe.impl.function.ApplyBiFunction;
import uk.gov.gchq.koryphe.impl.function.CallMethod;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.function.CsvLinesToMaps;
import uk.gov.gchq.koryphe.impl.function.IterableFunction;
import uk.gov.gchq.koryphe.impl.function.MapToTuple;
import uk.gov.gchq.koryphe.impl.function.MultiplyBy;
import uk.gov.gchq.koryphe.impl.function.ParseDate;
import uk.gov.gchq.koryphe.impl.function.ParseTime;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.function.ToString;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameCache;
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

public class RoadTrafficCsvElementGenerator2Test {
    @Test
    public void shouldParseSampleDataWithGenericFunctions() throws IOException {
        // Given
        CsvLinesToMaps parseCsv = new CsvLinesToMaps()
                .header("Region Name (GO)",
                        "ONS LACode",
                        "ONS LA Name",
                        "CP",
                        "S Ref E",
                        "S Ref N",
                        "Road",
                        "A-Junction",
                        "A Ref E",
                        "A Ref N",
                        "B-Junction",
                        "B Ref E",
                        "B Ref N",
                        "RCat",
                        "iDir",
                        "Year",
                        "dCount",
                        "Hour",
                        "PC",
                        "2WMV",
                        "CAR",
                        "BUS",
                        "LGV",
                        "HGVR2",
                        "HGVR3",
                        "HGVR4",
                        "HGVA3",
                        "HGVA5",
                        "HGVA6",
                        "HGV",
                        "AMV")
                .firstRow(1);

        IterableFunction<Map<String, Object>, Tuple<String>> toTuples = new IterableFunction<>(new MapToTuple<String>());

        IterableFunction<Tuple<String>, Tuple<String>> transformTuples = new IterableFunction<>(new And.Builder<>()
                .execute(new String[]{"Road", "A-Junction"}, new Concat(":"), new String[]{"A-Junction"})
                .execute(new String[]{"Road", "B-Junction"}, new Concat(":"), new String[]{"B-Junction"})
                .execute(new String[]{"A Ref E", "A Ref N"}, new Concat(), new String[]{"A-Location"})
                .execute(new String[]{"B Ref E", "B Ref N"}, new Concat(), new String[]{"B-Location"})
                .execute(new String[]{"THIS"}, new CreateRoadTrafficFreqMap(), new String[]{"countByVehicleType"})
                .execute(new String[]{"countByVehicleType"}, new CallMethod("getTotal"), new String[]{"total-count"})
                .execute(new String[]{"dCount"}, new ParseTime().timeZone("UTC"), new String[]{"timestamp"})
                .execute(new String[]{"Hour"}, new And<>(new ToInteger(), new MultiplyBy(60 * 60 * 1000), new ToLong()), new String[]{"hoursInMilliseconds"})
                .execute(new String[]{"timestamp", "hoursInMilliseconds"}, new And<>(new ApplyBiFunction<>(new Sum()), new ToString(), new ParseDate()), new String[]{"startDate"})
                .execute(new String[]{"startDate"}, new DateToTimeBucketEnd(CommonTimeUtil.TimeBucket.HOUR), new String[]{"endDate"})
                .build());

        TuplesToElements toElements = new TuplesToElements()
                .element(new ElementTupleDefinition("RegionContainsLocation")
                        .source("Region Name (GO)")
                        .destination("ONS LA Name"))
                .element(new ElementTupleDefinition("LocationContainsRoad")
                        .source("ONS LA Name")
                        .destination("Road"))
                .element(new ElementTupleDefinition("RoadHasJunction")
                        .source("Road")
                        .destination("A-Junction"))
                .element(new ElementTupleDefinition("RoadHasJunction")
                        .source("Road")
                        .destination("B-Junction"))
                .element(new ElementTupleDefinition("JunctionLocatedAt")
                        .source("A-Junction")
                        .destination("A-Location"))
                .element(new ElementTupleDefinition("JunctionLocatedAt")
                        .source("B-Junction")
                        .destination("B-Location"))
                .element(new ElementTupleDefinition("RoadUse")
                        .source("A-Junction")
                        .destination("B-Junction")
                        .property("startDate")
                        .property("endDate")
                        .property("countByVehicleType")
                        .property("count", "total-count"))
                .element(new ElementTupleDefinition("JunctionUse")
                        .vertex("A-Junction")
                        .property("startDate")
                        .property("endDate")
                        .property("countByVehicleType")
                        .property("count", "total-count"))
                .element(new ElementTupleDefinition("JunctionUse")
                        .vertex("B-Junction")
                        .property("startDate")
                        .property("endDate")
                        .property("countByVehicleType")
                        .property("count", "total-count"));

        HyperLogLogPlusElementGenerator addCardinalities = new HyperLogLogPlusElementGenerator().countProperty("count").edgeGroupProperty("edgeGroup");

        // Apply functions
        final And<List<String>, Iterable<Element>> generator2 = new And.Builder<List<String>, Iterable<Element>>()
                .execute(parseCsv)
                .execute(toTuples)
                .execute(transformTuples)
                .execute(toElements)
                .execute(addCardinalities)
                .build();
        // Uncomment the following for debugging
        // System.out.println(new String(JSONSerialiser.serialise(generator2, true)));

        final List<String> lines = IOUtils.readLines(createInputStream());
        final List<Element> elements2 = Lists.newArrayList(generator2.apply(lines));

        // Then - the results should be the same as those generated using the original element generator
        final RoadTrafficStringElementGenerator generator1 = new RoadTrafficStringElementGenerator();
        final List<Element> elements1;
        try (final InputStream inputStream = createInputStream()) {
            elements1 = Lists.newArrayList(generator1.apply(() -> new LineIterator(new InputStreamReader(inputStream))));
        }

        JSONSerialiser.getMapper();
        SimpleClassNameCache.setUseFullNameForSerialisation(false);
        elements1.forEach(e -> e.removeProperty("hllp"));
        elements2.forEach(e -> e.removeProperty("hllp"));
        ElementUtil.assertElementEquals(elements1, elements2);
    }

    private InputStream createInputStream() {
        return StreamUtil.openStream(getClass(), "/roadTrafficSampleData.csv");
    }

    public static class CreateRoadTrafficFreqMap extends KorypheFunction<Tuple<String>, FreqMap> {
        @Override
        public FreqMap apply(final Tuple<String> tuple) {
            final FreqMap freqMap = new FreqMap();
            for (final RoadTrafficDataField key : RoadTrafficDataField.VEHICLE_COUNTS) {
                final String fieldName = key.fieldName();
                final Object value = tuple.get(fieldName);
                freqMap.upsert(fieldName, Long.parseLong((String) value));
            }

            return freqMap;
        }
    }
}
