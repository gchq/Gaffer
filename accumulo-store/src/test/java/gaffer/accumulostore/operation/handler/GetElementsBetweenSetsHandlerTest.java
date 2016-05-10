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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.Operation;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GetElementsBetweenSetsHandlerTest {

    private final long TIMESTAMP = System.currentTimeMillis();
    // Query for all edges between the set {A0} and the set {A23}
    private final List<EntitySeed> seedsA = Arrays.asList(new EntitySeed("A0"));
    private final List<EntitySeed> seedsB = Arrays.asList(new EntitySeed("A23"));

    private View defaultView;
    private AccumuloStore byteEntityStore;
    private AccumuloStore gaffer1KeyStore;
    private Element expectedEdge1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEdge2 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEdge3 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEntity1 = new Entity(TestGroups.ENTITY, "A0");
    private Element expectedSummarisedEdge = new Edge(TestGroups.EDGE, "A0", "A23", true);

    @Before
    public void setup() throws StoreException, IOException {
        expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        expectedEdge1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

        expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
        expectedEdge2.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

        expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
        expectedEdge3.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

        expectedEntity1.putProperty(AccumuloPropertyNames.COUNT, 10000);
        expectedEntity1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 6);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COUNT, 3);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_4, 0);

        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .build();
        setupGraph(byteEntityStore);
        setupGraph(gaffer1KeyStore);
    }

    @After
    public void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void testNoSummarisation() throws OperationException {
        testNoSummarisation(byteEntityStore);
        testNoSummarisation(gaffer1KeyStore);
    }

    private void testNoSummarisation(final AccumuloStore store) throws OperationException {
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final Iterable<Element> elements = handler.doOperation(op, store);
        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }
        final List<Element> expectedResults = new ArrayList<>();

        expectedResults.add(expectedEdge1);
        expectedResults.add(expectedEdge2);
        expectedResults.add(expectedEdge2);
        expectedResults.add(expectedEntity1);

        for (final Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //Without query compaction the result size should be 4
        assertEquals(4, results.size());
    }

    @Test
    public void testShouldSummarise() throws OperationException {
        testShouldSummarise(byteEntityStore);
        testShouldSummarise(gaffer1KeyStore);
    }

    private void testShouldSummarise(final AccumuloStore store) throws OperationException {
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final Iterable<Element> elements = handler.doOperation(op, store);
        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }
        final List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(expectedSummarisedEdge);
        expectedResults.add(expectedEntity1);


        for (final Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //With query compaction the result size should be 2
        assertEquals(2, results.size());

    }

    @Test
    public void testShouldReturnOnlyEdgesWhenOptionSet() throws OperationException {
        testShouldReturnOnlyEdgesWhenOptionSet(byteEntityStore);
        testShouldReturnOnlyEdgesWhenOptionSet(gaffer1KeyStore);
    }

    private void testShouldReturnOnlyEdgesWhenOptionSet(final AccumuloStore store) throws OperationException {
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        op.setIncludeEdges(IncludeEdgeType.ALL);
        op.setIncludeEntities(false);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final Iterable<Element> elements = handler.doOperation(op, store);
        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }
        final List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(expectedSummarisedEdge);

        for (final Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //With query compaction the result size should be 1
        assertEquals(1, results.size());

        assertEquals(expectedResults, results);

    }

    @Test
    public void testShouldReturnOnlyEntitiesWhenOptionSet() throws OperationException {
        testShouldReturnOnlyEntitiesWhenOptionSet(byteEntityStore);
        testShouldReturnOnlyEntitiesWhenOptionSet(gaffer1KeyStore);
    }

    private void testShouldReturnOnlyEntitiesWhenOptionSet(final AccumuloStore store) throws OperationException {
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setIncludeEdges(IncludeEdgeType.NONE);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final Iterable<Element> elements = handler.doOperation(op, store);
        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }
        final List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(expectedEntity1);
        for (final Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //The result size should be 1
        assertEquals(1, results.size());

        assertEquals(expectedResults, results);
    }

    @Test
    public void testShouldSummariseOutGoingEdgesOnly() throws OperationException {
        testShouldSummariseOutGoingEdgesOnly(byteEntityStore);
        testShouldSummariseOutGoingEdgesOnly(gaffer1KeyStore);
    }

    private void testShouldSummariseOutGoingEdgesOnly(final AccumuloStore store) throws OperationException {
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final Iterable<Element> elements = handler.doOperation(op, store);
        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }

        //With query compaction the result size should be 2
        assertEquals(2, results.size());

        assertThat(results, IsCollectionContaining.hasItems(expectedEntity1, expectedSummarisedEdge));
    }

    @Test
    public void testShouldHaveNoIncomingEdges() throws OperationException {
        testShouldHaveNoIncomingEdges(byteEntityStore);
        testShouldHaveNoIncomingEdges(gaffer1KeyStore);
    }

    private void testShouldHaveNoIncomingEdges(final AccumuloStore store) throws OperationException {
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final Iterable<Element> elements = handler.doOperation(op, store);
        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }
        final List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(expectedEntity1);

        for (final Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }
        //The result size should be 1
        assertEquals(1, results.size());

        assertEquals(expectedResults, results);

    }

    private void setupGraph(final AccumuloStore store) {
        List<Element> data = new ArrayList<>();

        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        final Entity entity = new Entity(TestGroups.ENTITY, "A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            final Edge edge = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(edge);

            final Edge edge2 = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
            edge2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(edge2);

            final Edge edge3 = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(edge3);

            final Entity edgeEntity = new Entity(TestGroups.ENTITY, "A" + i);
            edgeEntity.putProperty(AccumuloPropertyNames.COUNT, i);
            edgeEntity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(edgeEntity);
        }
        addElements(data, store);
    }


    private void addElements(final Iterable<Element> data, final AccumuloStore store) {
        try {
            store.execute(new AddElements(data));
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}