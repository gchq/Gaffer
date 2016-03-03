/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.operation.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.TableExistsException;
import org.junit.BeforeClass;
import org.junit.Test;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.data.elementdefinition.view.ViewEntityDefinition;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;

public class GetElementsWithinSetHandlerTest {

    private static final String AUTHS = "Test";
    private static final long TIMESTAMP = System.currentTimeMillis();
    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore Gaffer1KeyStore;
    private static Element EXPECTED_EDGE_1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private static Element EXPECTED_EDGE_2 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private static Element EXPECTED_EDGE_3 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private static Element EXPECTED_ENTITY_1 = new Entity(TestGroups.ENTITY, "A0");
    private static Element EXPECTED_ENTITY_2 = new Entity(TestGroups.ENTITY, "A23");
    private static Element EXPECTED_SUMMARISED_EDGE = new Edge(TestGroups.EDGE, "A0", "A23", true);
    Set<EntitySeed> seeds = new HashSet<>(Arrays.asList(new EntitySeed("A0"), new EntitySeed("A23")));

    static {
        EXPECTED_EDGE_1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        EXPECTED_EDGE_1.putProperty(AccumuloPropertyNames.COUNT, 23);
        EXPECTED_EDGE_1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        EXPECTED_EDGE_2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
        EXPECTED_EDGE_2.putProperty(AccumuloPropertyNames.COUNT, 23);
        EXPECTED_EDGE_2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        EXPECTED_EDGE_3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
        EXPECTED_EDGE_3.putProperty(AccumuloPropertyNames.COUNT, 23);
        EXPECTED_EDGE_3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        EXPECTED_ENTITY_1.putProperty(AccumuloPropertyNames.COUNT, 10000);
        EXPECTED_ENTITY_1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        EXPECTED_ENTITY_2.putProperty(AccumuloPropertyNames.COUNT, 23);
        EXPECTED_ENTITY_2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        EXPECTED_SUMMARISED_EDGE.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1 * 3);
        EXPECTED_SUMMARISED_EDGE.putProperty(AccumuloPropertyNames.COUNT, 23 * 3);
        EXPECTED_SUMMARISED_EDGE.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
    }


    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        Gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();
        setupGraph(byteEntityStore);
        setupGraph(Gaffer1KeyStore);
    }

    @Test
    public void testNoSummarisation() throws OperationException {
        testNoSummarisation(byteEntityStore);
        testNoSummarisation(Gaffer1KeyStore);
    }

    public void testNoSummarisation(final AccumuloStore store) throws OperationException {
        GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.addOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, AUTHS);
        operation.setSummarise(false);
        GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);

        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }

        Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(EXPECTED_EDGE_1);
        expectedResults.add(EXPECTED_EDGE_2);
        expectedResults.add(EXPECTED_EDGE_3);
        expectedResults.add(EXPECTED_ENTITY_1);
        expectedResults.add(EXPECTED_ENTITY_2);

        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //Without query compaction the result size should be 5
        assertEquals(5, results.size());

    }

    @Test
    public void testShouldSummarise() throws OperationException {
        testShouldSummarise(byteEntityStore);
        testShouldSummarise(Gaffer1KeyStore);
    }

    public void testShouldSummarise(final AccumuloStore store) throws OperationException {
        GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setSummarise(true);
        GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);

        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }

        Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(EXPECTED_SUMMARISED_EDGE);
        expectedResults.add(EXPECTED_ENTITY_1);
        expectedResults.add(EXPECTED_ENTITY_2);

        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //After query compaction the result size should be 3
        assertEquals(3, results.size());

    }

    @Test
    public void testShouldReturnOnlyEdgesWhenOptionSet() throws OperationException {
        testShouldReturnOnlyEdgesWhenOptionSet(byteEntityStore);
        testShouldReturnOnlyEdgesWhenOptionSet(Gaffer1KeyStore);
    }

    public void testShouldReturnOnlyEdgesWhenOptionSet(final AccumuloStore store) throws OperationException {
        GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setIncludeEntities(false);
        operation.setSummarise(true);
        GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);

        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }

        Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(EXPECTED_SUMMARISED_EDGE);

        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //After query compaction the result size should be 1
        assertEquals(1, results.size());

    }

    @Test
    public void testShouldReturnOnlyEntitiesWhenOptionSet() throws OperationException {
        testShouldReturnOnlyEntitiesWhenOptionSet(byteEntityStore);
        testShouldReturnOnlyEntitiesWhenOptionSet(Gaffer1KeyStore);
    }

    public void testShouldReturnOnlyEntitiesWhenOptionSet(final AccumuloStore store) throws OperationException {
        GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setIncludeEdges(IncludeEdgeType.NONE);
        operation.setSummarise(true);
        GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);

        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }

        Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(EXPECTED_ENTITY_1);
        expectedResults.add(EXPECTED_ENTITY_2);

        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //The result size should be 2
        assertEquals(2, results.size());

    }

    private static void setupGraph(final AccumuloStore store) {
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(store);

            List<Element> data = new ArrayList<>();
            // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
            Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("A0");
            entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
            entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(entity);
            for (int i = 1; i < 100; i++) {
                Edge edge = new Edge(TestGroups.EDGE);
                edge.setSource("A0");
                edge.setDestination("A" + i);
                edge.setDirected(true);
                edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                edge.putProperty(AccumuloPropertyNames.COUNT, i);
                edge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                Edge edge2 = new Edge(TestGroups.EDGE);
                edge2.setSource("A0");
                edge2.setDestination("A" + i);
                edge2.setDirected(true);
                edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
                edge2.putProperty(AccumuloPropertyNames.COUNT, i);
                edge2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                Edge edge3 = new Edge(TestGroups.EDGE);
                edge3.setSource("A0");
                edge3.setDestination("A" + i);
                edge3.setDirected(true);
                edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
                edge3.putProperty(AccumuloPropertyNames.COUNT, i);
                edge3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                data.add(edge);
                data.add(edge2);
                data.add(edge3);
                entity = new Entity(TestGroups.ENTITY);
                entity.setVertex("A" + i);
                entity.putProperty(AccumuloPropertyNames.COUNT, i);
                entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                data.add(entity);
            }
            addElements(data, store);
        } catch (TableExistsException | StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    private static void addElements(final Iterable<Element> data, final AccumuloStore store) {
        try {
            store.execute(new AddElements(data));
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}