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
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.TableExistsException;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GetElementsWithinSetHandlerTest {

    private final String AUTHS = "Test";
    private final long TIMESTAMP = System.currentTimeMillis();
    private View defaultView;
    private AccumuloStore byteEntityStore;
    private AccumuloStore Gaffer1KeyStore;
    private Element expectedEdge1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEdge2 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEdge3 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEntity1 = new Entity(TestGroups.ENTITY, "A0");
    private Element expectedEntity2 = new Entity(TestGroups.ENTITY, "A23");
    private Element expectedSummarisedEdge = new Edge(TestGroups.EDGE, "A0", "A23", true);
    final Set<EntitySeed> seeds = new HashSet<>(Arrays.asList(new EntitySeed("A0"), new EntitySeed("A23")));

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
        expectedEntity2.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEntity2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1 * 3);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COUNT, 23 * 3);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        Gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
        setupGraph(byteEntityStore);
        setupGraph(Gaffer1KeyStore);
    }

    @Test
    public void testNoSummarisation() throws OperationException {
        testNoSummarisation(byteEntityStore);
        testNoSummarisation(Gaffer1KeyStore);
    }

    private void testNoSummarisation(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.addOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, AUTHS);
        operation.setSummarise(false);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, store);

        final List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }

        final Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(expectedEdge1);
        expectedResults.add(expectedEdge2);
        expectedResults.add(expectedEdge3);
        expectedResults.add(expectedEntity1);
        expectedResults.add(expectedEntity2);

        for (final Element expectedResult : expectedResults) {
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

    private void testShouldSummarise(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setSummarise(true);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, store);

        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }

        final Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(expectedSummarisedEdge);
        expectedResults.add(expectedEntity1);
        expectedResults.add(expectedEntity2);

        for (final Element expectedResult : expectedResults) {
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

    private void testShouldReturnOnlyEdgesWhenOptionSet(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setIncludeEntities(false);
        operation.setSummarise(true);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, store);

        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }

        final Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(expectedSummarisedEdge);

        for (final Element expectedResult : expectedResults) {
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

    private void testShouldReturnOnlyEntitiesWhenOptionSet(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setIncludeEdges(IncludeEdgeType.NONE);
        operation.setSummarise(true);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, store);

        final List<Element> results = new ArrayList<>();
        for (final Element elm : elements) {
            results.add(elm);
        }

        final Set<Element> expectedResults = new HashSet<>();
        expectedResults.add(expectedEntity1);
        expectedResults.add(expectedEntity2);

        for (final Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //The result size should be 2
        assertEquals(2, results.size());

    }

    private void setupGraph(final AccumuloStore store) {
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(store);

            final List<Element> data = new ArrayList<>();
            // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
            final Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("A0");
            entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
            entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(entity);
            for (int i = 1; i < 100; i++) {
                final Edge edge = new Edge(TestGroups.EDGE);
                edge.setSource("A0");
                edge.setDestination("A" + i);
                edge.setDirected(true);
                edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                edge.putProperty(AccumuloPropertyNames.COUNT, i);
                edge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

                final Edge edge2 = new Edge(TestGroups.EDGE);
                edge2.setSource("A0");
                edge2.setDestination("A" + i);
                edge2.setDirected(true);
                edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
                edge2.putProperty(AccumuloPropertyNames.COUNT, i);
                edge2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

                final Edge edge3 = new Edge(TestGroups.EDGE);
                edge3.setSource("A0");
                edge3.setDestination("A" + i);
                edge3.setDirected(true);
                edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
                edge3.putProperty(AccumuloPropertyNames.COUNT, i);
                edge3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                data.add(edge);
                data.add(edge2);
                data.add(edge3);

                final Entity edgeEntity = new Entity(TestGroups.ENTITY);
                edgeEntity.setVertex("A" + i);
                edgeEntity.putProperty(AccumuloPropertyNames.COUNT, i);
                edgeEntity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                data.add(edgeEntity);
            }
            addElements(data, store);
        } catch (TableExistsException | StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    private void addElements(final Iterable<Element> data, final AccumuloStore store) {
        try {
            store.execute(new AddElements(data));
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}