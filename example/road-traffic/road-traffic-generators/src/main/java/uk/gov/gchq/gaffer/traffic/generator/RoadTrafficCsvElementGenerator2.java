/*
 * Copyright 2016-2018 Crown Copyright
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

import org.apache.commons.lang3.time.DateUtils;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.function.PropertiesTransformer;
import uk.gov.gchq.gaffer.data.generator.CsvElementGenerator;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.HyperLogLogPlusElementGenerator;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.function.CallMethod;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.function.SetValue;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.tuple.function.KorypheFunction2;
import uk.gov.gchq.koryphe.util.DateUtil;

import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

@Since("1.8.0")
@Summary("String ElementGenerator for Road-Traffic demo")
public class RoadTrafficCsvElementGenerator2 implements ElementGenerator<String> {
    public static final CsvElementGenerator CSV_ELEMENT_GENERATOR = new CsvElementGenerator()
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
            .firstRow(1)
            .allFieldsRequired()
            .transformer(new PropertiesTransformer.Builder()
                    .select("Road", "A-Junction").execute(concat(":")).project("A-Junction")
                    .select("Road", "B-Junction").execute(concat(":")).project("B-Junction")
                    .select("A Ref E", "A Ref N").execute(new Concat()).project("A-Location")
                    .select("B Ref E", "B Ref N").execute(new Concat()).project("B-Location")
                    .select("PROPERTIES").execute(new CreateRoadTrafficFreqMap()).project("vehicle-counts")
                    .select("vehicle-counts").execute(new CallMethod("getTotal")).project("total-count")
                    .select("dCount").execute(new ToDate())
                    .select("Hour").execute(new ToInteger())
                    .select("dCount", "Hour").execute(new AddGivenHours()).project("start-date")
                    .select("start-date").execute(new AddHours(1)).project("end-date")
                    .build())
            .edge("RegionContainsLocation", new ElementTransformer.Builder()
                    .select("Region Name (GO)").project("SOURCE")
                    .select("ONS LA Name").project("DESTINATION")
                    .select().execute(new SetValue(true)).project("DIRECTED")
                    .build())
            .edge("LocationContainsRoad", new ElementTransformer.Builder()
                    .select("ONS LA Name").project("SOURCE")
                    .select("Road").project("DESTINATION")
                    .select().execute(new SetValue(true)).project("DIRECTED")
                    .build())
            .edge("RoadHasJunction", new ElementTransformer.Builder()
                    .select("Road").project("SOURCE")
                    .select("A-Junction").project("DESTINATION")
                    .select().execute(new SetValue(true)).project("DIRECTED")
                    .build())
            .edge("RoadHasJunction", new ElementTransformer.Builder()
                    .select("Road").project("SOURCE")
                    .select("B-Junction").project("DESTINATION")
                    .select().execute(new SetValue(true)).project("DIRECTED")
                    .build())
            .edge("JunctionLocatedAt", new ElementTransformer.Builder()
                    .select("A-Junction").project("SOURCE")
                    .select("A-Location").project("DESTINATION")
                    .select().execute(new SetValue(true)).project("DIRECTED")
                    .build())
            .edge("JunctionLocatedAt", new ElementTransformer.Builder()
                    .select("B-Junction").project("SOURCE")
                    .select("B-Location").project("DESTINATION")
                    .select().execute(new SetValue(true)).project("DIRECTED")
                    .build())
            .edge("RoadUse", new ElementTransformer.Builder()
                    .select("A-Junction").project("SOURCE")
                    .select("B-Junction").project("DESTINATION")
                    .select().execute(new SetValue(true)).project("DIRECTED")
                    .select("start-date").project("startDate")
                    .select("end-date").project("endDate")
                    .select("vehicle-counts").project("countByVehicleType")
                    .select("total-count").project("count")
                    .build())
            .entity("JunctionUse", new ElementTransformer.Builder()
                    .select("A-Junction").project("VERTEX")
                    .select("start-date").project("startDate")
                    .select("end-date").project("endDate")
                    .select("vehicle-counts").project("countByVehicleType")
                    .select("total-count").project("count")
                    .build())
            .entity("JunctionUse", new ElementTransformer.Builder()
                    .select("B-Junction").project("VERTEX")
                    .select("start-date").project("startDate")
                    .select("end-date").project("endDate")
                    .select("vehicle-counts").project("countByVehicleType")
                    .select("total-count").project("count")
                    .build())
            .followOnGenerator(new HyperLogLogPlusElementGenerator()
                    .transformer(new ElementTransformer.Builder()
                            .select("EDGE_GROUP").execute(new CollectionUtil.ToSingletonTreeSet()).project("edgeGroup")
                            .select().execute(new SetValue(1)).project("count")
                            .select("count").execute(new ToLong()).project("count")
                            .build()));

    @Override
    public Iterable<? extends Element> apply(final Iterable<? extends String> csvs) {
        return CSV_ELEMENT_GENERATOR.apply(csvs);
    }

    public static class ToDate extends KorypheFunction<String, Date> {
        @Override
        public Date apply(final String dCountString) {
            return DateUtil.parse(dCountString, TimeZone.getTimeZone(ZoneId.of("UTC")));
        }
    }

    public static class AddHours extends KorypheFunction<Date, Date> {
        private int hours;

        public AddHours() {
        }

        public AddHours(final int hours) {
            this.hours = hours;
        }

        @Override
        public Date apply(final Date date) {
            return DateUtils.addHours(date, hours);
        }

        public int getHours() {
            return hours;
        }

        public void setHours(final int hours) {
            this.hours = hours;
        }
    }

    public static class AddGivenHours extends KorypheFunction2<Date, Integer, Date> {
        @Override
        public Date apply(final Date date, final Integer hours) {
            return DateUtils.addHours(date, hours);
        }
    }

    public static class CreateRoadTrafficFreqMap extends KorypheFunction<Properties, FreqMap> {
        @Override
        public FreqMap apply(final Properties properties) {
            final FreqMap freqMap = new FreqMap();
            for (final RoadTrafficDataField key : RoadTrafficDataField.VEHICLE_COUNTS) {
                final String fieldName = key.fieldName();
                final Object value = properties.get(fieldName);
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
