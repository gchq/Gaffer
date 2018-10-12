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

import org.apache.commons.csv.CSVRecord;
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
@Summary("CSV ElementGenerator for Road-Traffic demo")
public class RoadTrafficCsvElementGenerator extends RoadTrafficElementGenerator<CSVRecord> {

    @Override
    public Iterable<Element> _apply(final CSVRecord record) {
        // Check that the record has the expected number of fields
        if (!record.isConsistent()) {
            return Collections.emptyList();
        }

        final FreqMap vehicleCountsByType = getVehicleCounts(record);
        final Date startDate = getDate(record.get(dCount.fieldName()), record.get(Hour.fieldName()));
        final Date endDate = null != startDate ? DateUtils.addHours(startDate, 1) : null;
        final String region = record.get(Region_Name.fieldName());
        final String location = record.get(ONS_LA_Name.fieldName());
        final String road = record.get(Road.fieldName());
        final String junctionA = road + ":" + record.get(A_Junction.fieldName());
        final String junctionB = road + ":" + record.get(B_Junction.fieldName());
        final String junctionALocation = record.get(A_Ref_E.fieldName()) + "," + record.get(A_Ref_N.fieldName());
        final String junctionBLocation = record.get(B_Ref_E.fieldName()) + "," + record.get(B_Ref_N.fieldName());

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
                        .build()
        );

        final List<Entity> entities = Arrays.asList(new Entity.Builder()
                        .group(ElementGroup.JUNCTION_USE)
                        .vertex(junctionA)
                        .property("countByVehicleType", vehicleCountsByType)
                        .property("startDate", startDate)
                        .property("endDate", endDate)
                        .property("count", getTotalCount(vehicleCountsByType))
                        .build(),

                new Entity.Builder()
                        .group(ElementGroup.JUNCTION_USE)
                        .vertex(junctionB)
                        .property("countByVehicleType", vehicleCountsByType)
                        .property("endDate", endDate)
                        .property("startDate", startDate)
                        .property("count", getTotalCount(vehicleCountsByType))
                        .build());

        final List<Entity> cardinalityEntities = createCardinalities(edges);

        // Create an iterable containing all the edges and entities
        return new ChainedIterable<>(edges, entities, cardinalityEntities);
    }

    private FreqMap getVehicleCounts(final CSVRecord record) {
        final FreqMap freqMap = new FreqMap();
        for (final RoadTrafficDataField field : RoadTrafficDataField.VEHICLE_COUNTS) {
            final String value = record.get(field.fieldName());
            freqMap.upsert(field.name(), value.isEmpty() ? 0 : Long.parseLong(value));
        }
        return freqMap;
    }
}
