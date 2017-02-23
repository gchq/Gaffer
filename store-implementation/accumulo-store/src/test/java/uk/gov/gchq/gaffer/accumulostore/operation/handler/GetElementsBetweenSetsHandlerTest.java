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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import com.google.common.collect.Iterables;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetElementsBetweenSetsHandlerTest {

    private final long TIMESTAMP = System.currentTimeMillis();
    // Query for all edges between the set {A0} and the set {A23}
    private final List<EntitySeed> seedsA = Arrays.asList(new EntitySeed("A0"));
    private final List<EntitySeed> seedsB = Arrays.asList(new EntitySeed("A23"));

    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(GetElementsBetweenSetsHandlerTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(GetElementsBetweenSetsHandlerTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(GetElementsBetweenSetsHandlerTest.class, "/accumuloStoreClassicKeys.properties"));

    private static final Element expectedEdge1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private static final Element expectedEdge2 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private static final Element expectedEdge3 = new Edge(TestGroups.EDGE, "A0", "A23", true);
    private static final Element expectedEntity1 = new Entity(TestGroups.ENTITY, "A0");
    private static final Element expectedSummarisedEdge = new Edge(TestGroups.EDGE, "A0", "A23", true);

    private User user = new User();

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
    }

    @Before
    public void reInitialise() throws StoreException {
        expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        expectedEdge1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_4, 0);

        expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
        expectedEdge2.putProperty(AccumuloPropertyNames.COUNT, 23);

        expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
        expectedEdge3.putProperty(AccumuloPropertyNames.COUNT, 23);

        expectedEntity1.putProperty(AccumuloPropertyNames.COUNT, 10000);

        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 6);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COUNT, 69);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_4, 0);

        defaultView = new View.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .build();

        byteEntityStore.initialise(schema, PROPERTIES);
        gaffer1KeyStore.initialise(schema, CLASSIC_PROPERTIES);
        setupGraph(byteEntityStore);
        setupGraph(gaffer1KeyStore);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
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
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<Element> elements = handler.doOperation(op, user, store);
        //Without query compaction the result size should be 4
        assertEquals(4, Iterables.size(elements));

        assertThat(elements, IsCollectionContaining.hasItems(expectedEdge1, expectedEdge2, expectedEdge3, expectedEntity1));
        elements.close();
    }

    @Test
    public void shouldReturnSummarisedElementsByteEntityStore() throws OperationException {
        shouldReturnSummarisedElements(byteEntityStore);
    }

    @Test
    public void shouldReturnSummarisedElementsGaffer1Store() throws OperationException {
        shouldReturnSummarisedElements(gaffer1KeyStore);
    }

    private void shouldReturnSummarisedElements(final AccumuloStore store) throws OperationException {
        final View opView = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, opView);

        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<Element> elements = handler.doOperation(op, user, store);

        //With query compaction the result size should be 2
        assertEquals(2, Iterables.size(elements));

        assertThat(elements, IsCollectionContaining.hasItems(expectedSummarisedEdge, expectedEntity1));
        elements.close();
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
        final View opView = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, opView);

        op.setIncludeEdges(IncludeEdgeType.ALL);
        op.setIncludeEntities(false);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<Element> elements = handler.doOperation(op, user, store);

        //With query compaction the result size should be 1
        assertEquals(1, Iterables.size(elements));

        assertThat(elements, IsCollectionContaining.hasItem(expectedSummarisedEdge));
        elements.close();
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
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setIncludeEdges(IncludeEdgeType.NONE);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<Element> elements = handler.doOperation(op, user, store);

        //The result size should be 1
        assertEquals(1, Iterables.size(elements));

        assertThat(elements, IsCollectionContaining.hasItem(expectedEntity1));
        elements.close();
    }

    @Test
    public void shouldSummariseOutGoingEdgesOnlyByteEntityStore() throws OperationException {
        shouldSummariseOutGoingEdgesOnly(byteEntityStore);
    }

    @Test
    public void shouldSummariseOutGoingEdgesOnlyGaffer1Store() throws OperationException {
        shouldSummariseOutGoingEdgesOnly(gaffer1KeyStore);
    }

    private void shouldSummariseOutGoingEdgesOnly(final AccumuloStore store) throws OperationException {
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, view);
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<Element> elements = handler.doOperation(op, user, store);

        //With query compaction the result size should be 2
        assertEquals(2, Iterables.size(elements));

        assertThat(elements, IsCollectionContaining.hasItems(expectedEntity1, expectedSummarisedEdge));
        elements.close();
    }

    @Test
    public void shouldHaveNoIncomingEdgesByteEntityStore() throws OperationException {
        shouldHaveNoIncomingEdges(byteEntityStore);
    }

    @Test
    public void shouldHaveNoIncomingEdgesGaffer1Store() throws OperationException {
        shouldHaveNoIncomingEdges(gaffer1KeyStore);
    }

    private void shouldHaveNoIncomingEdges(final AccumuloStore store) throws OperationException {
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, view);
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<Element> elements = handler.doOperation(op, user, store);

        //The result size should be 1
        assertEquals(1, Iterables.size(elements));

        assertThat(elements, IsCollectionContaining.hasItem(expectedEntity1));
        elements.close();
    }

    private static void setupGraph(final AccumuloStore store) {
        List<Element> data = new ArrayList<>();

        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        final Entity entity = new Entity(TestGroups.ENTITY, "A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            final Edge edge = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge.putProperty(AccumuloPropertyNames.COUNT, 23);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_4, 0);
            data.add(edge);

            final Edge edge2 = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 23);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
            data.add(edge2);

            final Edge edge3 = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 23);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            data.add(edge3);

            final Entity edgeEntity = new Entity(TestGroups.ENTITY, "A" + i);
            edgeEntity.putProperty(AccumuloPropertyNames.COUNT, i);
            data.add(edgeEntity);
        }
        addElements(data, store, new User());
    }


    private static void addElements(final Iterable<Element> data, final AccumuloStore store, final User user) {
        try {
            store.execute(new AddElements(data), user);
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}