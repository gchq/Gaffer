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

import com.google.common.collect.Iterables;
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
import gaffer.user.User;
import org.apache.accumulo.core.client.TableExistsException;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class GetElementsWithinSetHandlerTest {

    private final String AUTHS = "Test";
    private final long TIMESTAMP = System.currentTimeMillis();
    private View defaultView;
    private AccumuloStore byteEntityStore;
    private AccumuloStore gaffer1KeyStore;
    private Element expectedEdge1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEdge2 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEdge3 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private Element expectedEntity1 = new Entity(TestGroups.ENTITY, "A0");
    private Element expectedEntity2 = new Entity(TestGroups.ENTITY, "A23");
    private Element expectedSummarisedEdge = new Edge(TestGroups.EDGE, "A0", "A23", true);
    final Set<EntitySeed> seeds = new HashSet<>(Arrays.asList(new EntitySeed("A0"), new EntitySeed("A23")));

    private User user = new User();

    @Before
    public void setup() throws StoreException, IOException {
        expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        expectedEdge1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_4, 0);
//        expectedEdge1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
        expectedEdge2.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_4, 0);
//        expectedEdge2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
        expectedEdge3.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_4, 0);
//        expectedEdge3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        expectedEntity1.putProperty(AccumuloPropertyNames.COUNT, 10000);
//        expectedEntity1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        expectedEntity2.putProperty(AccumuloPropertyNames.COUNT, 23);
//        expectedEntity2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 6);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COUNT, 23 * 3);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_4, 0);
//        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
        setupGraph(byteEntityStore);
        setupGraph(gaffer1KeyStore);
    }

    @Test
    public void shouldReturnElementsNoSummarisationByteEntityStore() throws OperationException {
        shouldReturnElementsNoSummarisation(byteEntityStore);
    }

    @Test
    public void shouldReturnElementsNoSummarisationGaffer1Store() throws OperationException {
        shouldReturnElementsNoSummarisation(gaffer1KeyStore);
    }

    private void shouldReturnElementsNoSummarisation(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
//        operation.addOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, AUTHS);
        operation.setSummarise(false);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, user, store);

        //Without query compaction the result size should be 5
        assertEquals(5, Iterables.size(elements));
        assertThat(elements, IsCollectionContaining.hasItems(expectedEdge1, expectedEdge2, expectedEdge3, expectedEntity1, expectedEntity2));
    }

    @Test
    public void shouldSummariseByteEntityStore() throws OperationException {
        shouldSummarise(byteEntityStore);
    }

    @Test
    public void shouldSummariseGaffer1Store() throws OperationException {
        shouldSummarise(gaffer1KeyStore);
    }

    private void shouldSummarise(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setSummarise(true);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, user, store);

        //After query compaction the result size should be 3
        assertEquals(3, Iterables.size(elements));
        assertThat(elements, IsCollectionContaining.hasItems(expectedSummarisedEdge, expectedEntity1, expectedEntity2));
    }

    @Test
    public void shouldReturnOnlyEdgesWhenOptionSetByteEntityStore() throws OperationException {
        shouldReturnOnlyEdgesWhenOptionSet(byteEntityStore);
    }

    @Test
    public void shouldReturnOnlyEdgesWhenOptionSetGaffer1Store() throws OperationException {
        shouldReturnOnlyEdgesWhenOptionSet(gaffer1KeyStore);
    }

    private void shouldReturnOnlyEdgesWhenOptionSet(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setIncludeEntities(false);
        operation.setSummarise(true);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, user, store);

        final Collection<Element> forTest = new LinkedList<>();
        Iterables.addAll(forTest, elements);

        //After query compaction the result size should be 1
        assertEquals(1, Iterables.size(elements));
        assertThat(elements, IsCollectionContaining.hasItem(expectedSummarisedEdge));

    }

    @Test
    public void shouldReturnOnlyEntitiesWhenOptionSetByteEntityStore() throws OperationException {
        shouldReturnOnlyEntitiesWhenOptionSet(byteEntityStore);
    }

    @Test
    public void shouldReturnOnlyEntitiesWhenOptionSetGaffer1Store() throws OperationException {
        shouldReturnOnlyEntitiesWhenOptionSet(gaffer1KeyStore);
    }

    private void shouldReturnOnlyEntitiesWhenOptionSet(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet<>(defaultView, seeds);
        operation.setIncludeEdges(IncludeEdgeType.NONE);
        operation.setSummarise(true);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();
        final Iterable<Element> elements = handler.doOperation(operation, user, store);

        //The result size should be 2
        assertEquals(2, Iterables.size(elements));
        assertThat(elements, IsCollectionContaining.hasItems(expectedEntity1, expectedEntity2));
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
//            entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(entity);
            for (int i = 1; i < 100; i++) {
                final Edge edge = new Edge(TestGroups.EDGE);
                edge.setSource("A0");
                edge.setDestination("A" + i);
                edge.setDirected(true);
                edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                edge.putProperty(AccumuloPropertyNames.COUNT, i);
                edge.putProperty(AccumuloPropertyNames.PROP_1, 0);
                edge.putProperty(AccumuloPropertyNames.PROP_2, 0);
                edge.putProperty(AccumuloPropertyNames.PROP_3, 0);
                edge.putProperty(AccumuloPropertyNames.PROP_4, 0);
//                edge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

                final Edge edge2 = new Edge(TestGroups.EDGE);
                edge2.setSource("A0");
                edge2.setDestination("A" + i);
                edge2.setDirected(true);
                edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
                edge2.putProperty(AccumuloPropertyNames.COUNT, i);
                edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
                edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
                edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
                edge2.putProperty(AccumuloPropertyNames.PROP_4, 0);
//                edge2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);

                final Edge edge3 = new Edge(TestGroups.EDGE);
                edge3.setSource("A0");
                edge3.setDestination("A" + i);
                edge3.setDirected(true);
                edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
                edge3.putProperty(AccumuloPropertyNames.COUNT, i);
                edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
                edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
                edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
                edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);
//                edge3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                data.add(edge);
                data.add(edge2);
                data.add(edge3);

                final Entity edgeEntity = new Entity(TestGroups.ENTITY);
                edgeEntity.setVertex("A" + i);
                edgeEntity.putProperty(AccumuloPropertyNames.COUNT, i);
//                edgeEntity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
                data.add(edgeEntity);
            }
            final User user = new User();
            addElements(data, user, store);
        } catch (TableExistsException | StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    private void addElements(final Iterable<Element> data, final User user, final AccumuloStore store) {
        try {
            store.execute(new AddElements(data), user);
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}