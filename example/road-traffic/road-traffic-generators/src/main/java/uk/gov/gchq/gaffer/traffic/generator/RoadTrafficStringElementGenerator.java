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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.traffic.ElementGroup;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.A_Junction;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.A_Ref_E;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.A_Ref_N;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.B_Junction;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.B_Ref_E;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.B_Ref_N;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.Hour;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.ONS_LA_Name;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.Region_Name;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.Road;
import static uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.dCount;

@Since("1.0.0")
@Summary("String ElementGenerator for Road-Traffic demo")
public class RoadTrafficStringElementGenerator extends RoadTrafficElementGenerator<String> {

    @Override
    public Iterable<Element> _apply(final String line) {
        final String[] fields = extractFields(line);
        if (null == fields) {
            return Collections.emptyList();
        }

        // Extract required fields
        final FreqMap vehicleCountsByType = getVehicleCounts(fields);
        final Date startDate = getDate(fields[dCount.ordinal()], fields[Hour.ordinal()]);
        final Date endDate = null != startDate ? DateUtils.addHours(startDate, 1) : null;
        final String region = fields[Region_Name.ordinal()];
        final String location = fields[ONS_LA_Name.ordinal()];
        final String road = fields[Road.ordinal()];
        final String junctionA = road + ":" + fields[A_Junction.ordinal()];
        final String junctionB = road + ":" + fields[B_Junction.ordinal()];
        final String junctionALocation = fields[A_Ref_E.ordinal()] + "," + fields[A_Ref_N.ordinal()];
        final String junctionBLocation = fields[B_Ref_E.ordinal()] + "," + fields[B_Ref_N.ordinal()];

        final List<Edge> edges = Arrays.asList(
                new Edge.Builder()
                        .group(ElementGroup.REGION_CONTAINS_LOCATION)
                        .source(region)
                        .dest(location)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group(ElementGroup.LOCATION_CONTAINS_ROAD)
                        .source(location)
                        .dest(road)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group(ElementGroup.ROAD_HAS_JUNCTION)
                        .source(road)
                        .dest(junctionA)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group(ElementGroup.ROAD_HAS_JUNCTION)
                        .source(road)
                        .dest(junctionB)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group(ElementGroup.JUNCTION_LOCATED_AT)
                        .source(junctionA)
                        .dest(junctionALocation)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group(ElementGroup.JUNCTION_LOCATED_AT)
                        .source(junctionB)
                        .dest(junctionBLocation)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group(ElementGroup.ROAD_USE)
                        .source(junctionA)
                        .dest(junctionB)
                        .directed(true)
                        .property("startDate", startDate)
                        .property("endDate", endDate)
                        .property("count", getTotalCount(vehicleCountsByType))
                        .property("countByVehicleType", vehicleCountsByType)
                        .build());

        final List<Entity> entities = Arrays.asList(
                new Entity.Builder()
                        .group(ElementGroup.JUNCTION_USE)
                        .vertex(junctionA)
                        .property("countByVehicleType", vehicleCountsByType)
                        .property("endDate", endDate)
                        .property("startDate", startDate)
                        .property("count", getTotalCount(vehicleCountsByType))
                        .build(),

                new Entity.Builder()
                        .group(ElementGroup.JUNCTION_USE)
                        .vertex(junctionB)
                        .property("countByVehicleType", vehicleCountsByType)
                        .property("endDate", endDate)
                        .property("startDate", startDate)
                        .property("count", getTotalCount(vehicleCountsByType))
                        .build()
        );

        final List<Entity> cardinalityEntities = createCardinalities(edges);

        return new ChainedIterable<>(edges, entities, cardinalityEntities);
    }

    private FreqMap getVehicleCounts(final String[] fields) {
        final FreqMap freqMap = new FreqMap();
        for (final RoadTrafficDataField fieldName : RoadTrafficDataField.VEHICLE_COUNTS) {
            Long value;
            try {
                value = Long.parseLong(fields[fieldName.ordinal()]);
            } catch (final ArrayIndexOutOfBoundsException e) {
                value = 0L;
            }

            freqMap.upsert(fieldName.name(), value);
        }
        return freqMap;
    }

    public static boolean isHeader(final String line) {
        return line.startsWith("\"Region Name (GO)\",");
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "private method and the null result is handled properly")
    public static String[] extractFields(final String line) {
        if (isHeader(line)) {
            return null;
        }

        final String trimStart = StringUtils.removeStart(line, "\"");
        final String trimEnd = StringUtils.removeEnd(trimStart, "\"");
        final String[] fields = trimEnd.split("\",\"");

        // Must pad out the list of fields due to a change to the input data
        final List<String> expandedFields = new LinkedList<String>(Arrays.asList(fields));
        expandedFields.add(6, "");
        expandedFields.add(7, "");

        if (expandedFields.size() != uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataField.values().length) {
            return null;
        }

        return expandedFields.toArray(new String[]{});
    }
}
