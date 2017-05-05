/*
 * Copyright 2016 Crown Copyright
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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.lang3.time.DateUtils;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import uk.gov.gchq.gaffer.traffic.ElementGroup;
import uk.gov.gchq.gaffer.types.FreqMap;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

public class RoadTrafficElementGenerator implements OneToManyElementGenerator<String> {

    @Override
    public Iterable<Element> _apply(final String line) {
        final String[] fields = RoadTrafficDataField.extractFields(line);
        if (null == fields) {
            return Collections.emptyList();
        }

        final FreqMap vehicleCountsByType = getVehicleCounts(fields);
        final Date startDate = getDate(fields[dCount.index()], fields[Hour.index()]);
        final Date endDate = null != startDate ? DateUtils.addHours(startDate, 1) : null;
        final String region = fields[Region_Name.index()];
        final String location = fields[ONS_LA_Name.index()];
        final String road = fields[Road.index()];
        final String junctionA = road + ":" + fields[A_Junction.index()];
        final String junctionB = road + ":" + fields[B_Junction.index()];
        final String junctionALocation = fields[A_Ref_E.index()] + "," + fields[A_Ref_N.index()];
        final String junctionBLocation = fields[B_Ref_E.index()] + "," + fields[B_Ref_N.index()];

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

    private List<Entity> createCardinalities(final List<Edge> edges) {
        final List<Entity> cardinalities = new ArrayList<>(edges.size() * 2);

        for (final Edge edge : edges) {
            cardinalities.add(createCardinality(edge.getSource(), edge.getDestination(), edge));
            cardinalities.add(createCardinality(edge.getDestination(), edge.getSource(), edge));
        }

        return cardinalities;
    }

    private Entity createCardinality(final Object source,
                                     final Object destination,
                                     final Edge edge) {
        final HyperLogLogPlus hllp = new HyperLogLogPlus(5, 5);
        hllp.offer(destination);

        return new Entity.Builder()
                .vertex(source)
                .group("Cardinality")
                .property("edgeGroup", CollectionUtil.treeSet(edge.getGroup()))
                .property("hllp", hllp)
                .property("count", 1L)
                .build();
    }

    private FreqMap getVehicleCounts(final String[] fields) {
        final FreqMap freqMap = new FreqMap();
        for (final RoadTrafficDataField fieldName : RoadTrafficDataField.VEHICLE_COUNTS) {
            freqMap.upsert(fieldName.name(), Long.parseLong(fields[fieldName.index()]));
        }
        return freqMap;
    }

    private long getTotalCount(final FreqMap freqmap) {
        long sum = 0;
        for (final Long count : freqmap.values()) {
            sum += count;
        }

        return sum;
    }

    private Date getDate(final String dCountString, final String hour) {
        Date dCount = null;
        try {
            dCount = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse(dCountString);
        } catch (ParseException e) {
            // incorrect date format
        }

        if (null == dCount) {
            try {
                dCount = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dCountString);
            } catch (ParseException e) {
                // another incorrect date format
            }
        }

        if (null == dCount) {
            return null;
        }

        return DateUtils.addHours(dCount, Integer.parseInt(hour));
    }
}
