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

package uk.gov.gchq.gaffer.traffic;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.generator.CsvGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.function.FreqMapExtractor;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.predicate.PredicateMap;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Runs queries against a Gaffer store containing the sample Road Traffic data to ensure that it has been ingested
 * correctly and that the store is capable of returning the correct answer when queries involving filters, aggregations,
 * transformations etc are submitted.
 */
public abstract class RoadTrafficTestQueries {

    private static final Logger LOGGER = Logger.getLogger(RoadTrafficTestQueries.class.getName());

    protected Graph graph;
    protected User user;

    @Before
    public abstract void prepareProxy() throws IOException;

    @Test
    public void checkM4JunctionCount() throws OperationException {
        assertNotNull("graph is null", this.graph);

        final GetElements query = new GetElements.Builder()
                .input(new EntitySeed("M4"))
                .view(new View.Builder()
                        .edge("RoadHasJunction")
                        .build())
                .build();

        try (final CloseableIterable<? extends Element> elements = this.graph.execute(query, this.user)) {
            int x = 0;
            for (final Element element : elements) {
                x++;
            }
            assertEquals(14, x);
        }
    }

    private static final Properties M4_JUNCTION_17_TO_16_PROPERTIES;

    static {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date startDate = null;
        try {
            startDate = dateFormat.parse("2000-10-10 07:00:00");
        } catch (final ParseException e) {
            LOGGER.info("Error parsing startDate: " + e.getMessage());
        }

        Date endDate = null;
        try {
            endDate = dateFormat.parse("2000-10-10 08:00:00");
        } catch (final ParseException e) {
            LOGGER.info("Error parsing endDate: " + e.getMessage());
        }

        final FreqMap countByVehicleType = new FreqMap();
        countByVehicleType.put("PC", 0L);
        countByVehicleType.put("WMV2", 13L);
        countByVehicleType.put("CAR", 4072L);
        countByVehicleType.put("BUS", 19L);
        countByVehicleType.put("LGV", 727L);
        countByVehicleType.put("HGV", 693L);
        countByVehicleType.put("HGVA3", 60L);
        countByVehicleType.put("HGVA5", 220L);
        countByVehicleType.put("HGVA6", 159L);
        countByVehicleType.put("HGVR2", 183L);
        countByVehicleType.put("HGVR3", 28L);
        countByVehicleType.put("HGVR4", 43L);
        countByVehicleType.put("AMV", 5524L);

        M4_JUNCTION_17_TO_16_PROPERTIES = new Properties();
        M4_JUNCTION_17_TO_16_PROPERTIES.put("startDate", startDate);
        M4_JUNCTION_17_TO_16_PROPERTIES.put("endDate", endDate);
        M4_JUNCTION_17_TO_16_PROPERTIES.put("count", 11741L);
        M4_JUNCTION_17_TO_16_PROPERTIES.put("countByVehicleType", countByVehicleType);
    }

    @Test
    public void checkM4Junction17To16RoadUse() throws OperationException, ParseException {
        assumeTrue("Skipping test as the store does not implement required trait.", this.graph.hasTrait(StoreTrait.INGEST_AGGREGATION));
        assumeTrue("Skipping test as the store does not implement required trait.", this.graph.hasTrait(StoreTrait.PRE_AGGREGATION_FILTERING));
        assertNotNull("graph is null", this.graph);

        final GetElements query = new GetElements.Builder()
                .input(new EdgeSeed("M4:17", "M4:16", true))
                .view(new View.Builder()
                        .edge("RoadUse", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("startDate")
                                        .execute(new IsEqual(M4_JUNCTION_17_TO_16_PROPERTIES.get("startDate")))
                                        .select("endDate")
                                        .execute(new IsEqual(M4_JUNCTION_17_TO_16_PROPERTIES.get("endDate")))
                                        .build())
                                .build())
                        .build())
                .build();

        try (final CloseableIterable<? extends Element> elements = this.graph.execute(query, this.user)) {
            CloseableIterator<? extends Element> iter = elements.iterator();
            assertTrue(iter.hasNext());

            final Element element = iter.next();
            assertFalse("Expected query to return only 1 element, but it has returned multiple!", iter.hasNext());

            assertEquals(M4_JUNCTION_17_TO_16_PROPERTIES, element.getProperties());
        }
    }

    private static final Properties M4_JUNCTION_16_PROPERTIES;

    static {
        final FreqMap countByVehicleType = new FreqMap();
        countByVehicleType.put("PC", 18L);
        countByVehicleType.put("WMV2", 2675L);
        countByVehicleType.put("CAR", 531901L);
        countByVehicleType.put("BUS", 3923L);
        countByVehicleType.put("LGV", 76361L);
        countByVehicleType.put("HGV", 82853L);
        countByVehicleType.put("HGVA3", 6820L);
        countByVehicleType.put("HGVA5", 29692L);
        countByVehicleType.put("HGVA6", 17395L);
        countByVehicleType.put("HGVR2", 22623L);
        countByVehicleType.put("HGVR3", 3215L);
        countByVehicleType.put("HGVR4", 3108L);
        countByVehicleType.put("AMV", 697713L);

        M4_JUNCTION_16_PROPERTIES = new Properties();
        M4_JUNCTION_16_PROPERTIES.put("startDate", new Date(971161200000L));
        M4_JUNCTION_16_PROPERTIES.put("endDate", new Date(1337886000000L));
        M4_JUNCTION_16_PROPERTIES.put("count", 1478297L);
        M4_JUNCTION_16_PROPERTIES.put("countByVehicleType", countByVehicleType);
    }

    @Test
    public void checkM4Junction16Use() throws OperationException {
        assumeTrue("Skipping test as the store does not implement required trait.", this.graph.hasTrait(StoreTrait.QUERY_AGGREGATION));
        assertNotNull("graph is null", this.graph);

        final GetElements query = new GetElements.Builder()
                .input(new EntitySeed("M4:16"))
                .view(new View.Builder()
                        .entity("JunctionUse", new ViewElementDefinition.Builder()
                                .groupBy()
                                .build())
                        .build())
                .build();

        try (final CloseableIterable<? extends Element> elements = this.graph.execute(query, this.user)) {
            CloseableIterator<? extends Element> iter = elements.iterator();
            assertTrue(iter.hasNext());

            final Element element = iter.next();
            assertFalse("Expected query to return only 1 element, but it has returned multiple!", iter.hasNext());

            assertEquals(M4_JUNCTION_16_PROPERTIES, element.getProperties());
        }
    }

    private static final Set<String> SW_ROAD_JUNCTIONS_WITH_HEAVY_BUS_USAGE_IN_2000;

    static {
        SW_ROAD_JUNCTIONS_WITH_HEAVY_BUS_USAGE_IN_2000 = new HashSet<>(4);
        SW_ROAD_JUNCTIONS_WITH_HEAVY_BUS_USAGE_IN_2000.add("Junction,Bus Count");
        SW_ROAD_JUNCTIONS_WITH_HEAVY_BUS_USAGE_IN_2000.add("M32:2,1411");
        SW_ROAD_JUNCTIONS_WITH_HEAVY_BUS_USAGE_IN_2000.add("M4:LA Boundary,1958");
        SW_ROAD_JUNCTIONS_WITH_HEAVY_BUS_USAGE_IN_2000.add("M5:LA Boundary,1067");
    }

    @Test
    public void checkRoadJunctionsInSouthWestHeavilyUsedByBusesIn2000() throws OperationException, ParseException {
        assumeTrue("Skipping test as the store does not implement required trait.", this.graph.hasTrait(StoreTrait.QUERY_AGGREGATION));
        assumeTrue("Skipping test as the store does not implement required trait.", this.graph.hasTrait(StoreTrait.TRANSFORMATION));
        assumeTrue("Skipping test as the store does not implement required trait.", this.graph.hasTrait(StoreTrait.PRE_AGGREGATION_FILTERING));
        assumeTrue("Skipping test as the store does not implement required trait.", this.graph.hasTrait(StoreTrait.POST_AGGREGATION_FILTERING));
        assertNotNull("graph is null", this.graph);

        final Date JAN_01_2000 = new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01");
        final Date JAN_01_2001 = new SimpleDateFormat("yyyy-MM-dd").parse("2001-01-01");

        final OperationChain<Iterable<? extends String>> opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds.Builder()
                        .input(new EntitySeed("South West"))
                        .view(new View.Builder()
                                .edge("RegionContainsLocation")
                                .build())
                        .build())
                .then(new GetAdjacentIds.Builder()
                        .view(new View.Builder()
                                .edge("LocationContainsRoad")
                                .build())
                        .build())
                .then(new ToSet<>())
                .then(new GetAdjacentIds.Builder()
                        .view(new View.Builder()
                                .edge("RoadHasJunction")
                                .build())
                        .build())
                .then(new GetElements.Builder()
                        .view(new View.Builder()
                                .globalElements(new GlobalViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .entity("JunctionUse", new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("startDate")
                                                .execute(new IsMoreThan(JAN_01_2000, true))
                                                .select("endDate")
                                                .execute(new IsLessThan(JAN_01_2001, false))
                                                .build())
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select("countByVehicleType")
                                                .execute(new PredicateMap<>("BUS", new IsMoreThan(1000L)))
                                                .build())

                                                // Extract the bus count out of the frequency map and store in transient property "busCount"
                                        .transientProperty("busCount", Long.class)
                                        .transformer(new ElementTransformer.Builder()
                                                .select("countByVehicleType")
                                                .execute(new FreqMapExtractor("BUS"))
                                                .project("busCount")
                                                .build())
                                        .build())
                                .build())
                        .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                        .build())
                        // Convert the result entities to a simple CSV in format: Junction,busCount.
                .then(new ToCsv.Builder()
                        .generator(new CsvGenerator.Builder()
                                .vertex("Junction")
                                .property("busCount", "Bus Count")
                                .build())
                        .build())
                .build();

        Set<String> resultSet = new HashSet<>();

        final Iterable<? extends String> results = this.graph.execute(opChain, this.user);
        for (final String r : results) {
            resultSet.add(r);
        }

        assertEquals(SW_ROAD_JUNCTIONS_WITH_HEAVY_BUS_USAGE_IN_2000, resultSet);
    }

}
