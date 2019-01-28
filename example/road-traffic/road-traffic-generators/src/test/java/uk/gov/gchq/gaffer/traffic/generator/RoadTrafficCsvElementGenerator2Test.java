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
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.function.DateToTimeBucketEnd;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition;
import uk.gov.gchq.gaffer.data.element.function.TuplesToElements;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.HyperLogLogPlusElementGenerator;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.function.CallMethod;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.function.CsvLinesToMaps;
import uk.gov.gchq.koryphe.impl.function.IterableFunction;
import uk.gov.gchq.koryphe.impl.function.MapToTuple;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameCache;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.function.KorypheFunction2;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunctionComposite;
import uk.gov.gchq.koryphe.util.DateUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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

        IterableFunction<Tuple<String>, Tuple<String>> transformTuples = new IterableFunction<>(new TupleAdaptedFunctionComposite.Builder<>()
                .select(new String[]{"Road", "A-Junction"}).execute(concat(":")).project(new String[]{"A-Junction"})
                .select(new String[]{"Road", "B-Junction"}).execute(concat(":")).project(new String[]{"B-Junction"})
                .select(new String[]{"A Ref E", "A Ref N"}).execute(new Concat()).project(new String[]{"A-Location"})
                .select(new String[]{"B Ref E", "B Ref N"}).execute(new Concat()).project(new String[]{"B-Location"})
                .select(new String[]{"THIS"}).execute(new CreateRoadTrafficFreqMap()).project(new String[]{"countByVehicleType"})
                .select(new String[]{"countByVehicleType"}).execute(new CallMethod("getTotal")).project(new String[]{"total-count"})
                .select(new String[]{"dCount"}).execute(new ToDate()).project(new String[]{"dCount"})
                .select(new String[]{"Hour"}).execute(new ToInteger()).project(new String[]{"Hour"})
                .select(new String[]{"dCount", "Hour"}).execute(new AddGivenHours()).project(new String[]{"startDate"})
                .select(new String[]{"startDate"}).execute(new DateToTimeBucketEnd(CommonTimeUtil.TimeBucket.HOUR)).project(new String[]{"endDate"})
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

        // Uncomment the following for debugging
//         System.out.println(new String(JSONSerialiser.serialise(Arrays.asList(
//                 parseCsv,
//                 toTuples,
//                 transformTuples,
//                 toElements,
//                 addCardinalities
//         ), true)));

        // Apply functions
        List<String> lines = IOUtils.readLines(createInputStream());
        Iterable<Map<String, Object>> maps = parseCsv.apply(lines);
        Iterable<Tuple<String>> tuples = toTuples.apply(maps);
        tuples = transformTuples.apply(tuples);
        Iterable<Element> elements2 = toElements.apply(tuples);
        elements2 = (Iterable<Element>) addCardinalities.apply(elements2);
        elements2 = Lists.newArrayList(elements2);

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

    public static class ToDate extends KorypheFunction<String, Date> {
        @Override
        public Date apply(final String dCountString) {
            return DateUtil.parse(dCountString, TimeZone.getTimeZone(ZoneId.of("UTC")));
        }
    }

    public static class AddGivenHours extends KorypheFunction2<Date, Integer, Date> {
        @Override
        public Date apply(final Date date, final Integer hours) {
            return DateUtils.addHours(date, hours);
        }
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

    private static Concat concat(final String separator) {
        final Concat concat = new Concat();
        concat.setSeparator(separator);
        return concat;
    }
}
