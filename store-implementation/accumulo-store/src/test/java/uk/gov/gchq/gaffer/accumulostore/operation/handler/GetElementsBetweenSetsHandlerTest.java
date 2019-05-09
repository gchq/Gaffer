/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
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
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GetElementsBetweenSetsHandlerTest {
    // Query for all edges between the set {A0} and the set {A23}
    private final List<EntityId> inputA = Collections.singletonList(new EntitySeed("A0"));
    private final List<EntityId> inputB = Collections.singletonList(new EntitySeed("A23"));

    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(GetElementsBetweenSetsHandlerTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(GetElementsBetweenSetsHandlerTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(GetElementsBetweenSetsHandlerTest.class, "/accumuloStoreClassicKeys.properties"));

    private final Element expectedEdge1 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A23")
                    .directed(true)
                    .build();
    private final Element expectedEdge2 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A23")
                    .directed(true)
                    .build();
    private final Element expectedEdge3 =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A23")
                    .directed(true)
                    .build();
    private final Element expectedEntity1 =
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A0")
                    .build();
    private final Element expectedEntity1B =
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A23")
                    .build();
    private final Element expectedSummarisedEdge =
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A23")
                    .directed(true)
                    .build();

    private User user = new User();

    @BeforeClass
    public static void setup() {
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
        expectedEntity1B.putProperty(AccumuloPropertyNames.COUNT, 23);

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

        byteEntityStore.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
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
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(inputA).inputB(inputB).view(defaultView).build();
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<? extends Element> elements = handler.doOperation(op, user, store);

        final Set<Element> elementsSet = Sets.newHashSet(elements);

        //Without query compaction the result size should be 4
        assertEquals(Sets.newHashSet(expectedEdge1, expectedEdge2, expectedEdge3, expectedEntity1), elementsSet);
        for (final Element element : elementsSet) {
            if (element instanceof Edge) {
                assertEquals(EdgeId.MatchedVertex.SOURCE, ((Edge) element).getMatchedVertex());
            }
        }
    }

    @Test
    public void shouldReturnElementsNoSummarisationByteEntityStoreMatchedAsDestination() throws OperationException {
        shouldReturnElementsNoSummarisationMatchedAsDestination(byteEntityStore);
    }

    @Test
    public void shouldReturnElementsNoSummarisationGaffer1StoreMatchedAsDestination() throws OperationException {
        shouldReturnElementsNoSummarisationMatchedAsDestination(gaffer1KeyStore);
    }

    private void shouldReturnElementsNoSummarisationMatchedAsDestination(final AccumuloStore store) throws OperationException {
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(inputB).inputB(inputA).view(defaultView).build();
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<? extends Element> elements = handler.doOperation(op, user, store);

        final Set<Element> elementsSet = Sets.newHashSet(elements);

        //Without query compaction the result size should be 4
        assertEquals(Sets.newHashSet(expectedEdge1, expectedEdge2, expectedEdge3, expectedEntity1B), elementsSet);
        for (final Element element : elementsSet) {
            if (element instanceof Edge) {
                assertEquals(EdgeId.MatchedVertex.DESTINATION, ((Edge) element).getMatchedVertex());
            }
        }
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

        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(inputA).inputB(inputB).view(opView).build();

        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<? extends Element> elements = handler.doOperation(op, user, store);

        //With query compaction the result size should be 2
        assertEquals(2, Iterables.size(elements));

        assertTrue(Iterables.contains(elements, expectedSummarisedEdge));
        assertTrue(Iterables.contains(elements, expectedEntity1));
        elements.close();
    }

    @Test
    public void shouldReturnOnlyEdgesWhenOptionSetByteEntityStore() throws OperationException {
        shouldReturnOnlyEdgesWhenViewContainsNoEntities(byteEntityStore);
    }

    @Test
    public void shouldReturnOnlyEdgesWhenOptionSetGaffer1Store() throws OperationException {
        shouldReturnOnlyEdgesWhenViewContainsNoEntities(gaffer1KeyStore);
    }

    private void shouldReturnOnlyEdgesWhenViewContainsNoEntities(final AccumuloStore store) throws OperationException {
        final View opView = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(inputA).inputB(inputB).view(opView).build();

        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<? extends Element> elements = handler.doOperation(op, user, store);

        //With query compaction the result size should be 1
        assertEquals(1, Iterables.size(elements));

        assertTrue(Iterables.contains(elements, expectedSummarisedEdge));
        elements.close();
    }

    @Test
    public void shouldReturnOnlyEntitiesWhenOptionSetByteEntityStore() throws OperationException {
        shouldReturnOnlyEntitiesWhenViewContainsNoEdges(byteEntityStore);
    }

    @Test
    public void shouldReturnOnlyEntitiesWhenOptionSetGaffer1Store() throws OperationException {
        shouldReturnOnlyEntitiesWhenViewContainsNoEdges(gaffer1KeyStore);
    }

    private void shouldReturnOnlyEntitiesWhenViewContainsNoEdges(final AccumuloStore store) throws OperationException {
        final View opView = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(inputA).inputB(inputB).view(opView).build();
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<? extends Element> elements = handler.doOperation(op, user, store);

        //The result size should be 1
        assertEquals(1, Iterables.size(elements));

        assertTrue(Iterables.contains(elements, expectedEntity1));
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
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(inputA).inputB(inputB).view(view).build();
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<? extends Element> elements = handler.doOperation(op, user, store);

        //With query compaction the result size should be 2
        assertEquals(2, Iterables.size(elements));

        assertTrue(Iterables.contains(elements, expectedEntity1));
        assertTrue(Iterables.contains(elements, expectedSummarisedEdge));
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
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(inputA).inputB(inputB).view(view).build();
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        final GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        final CloseableIterable<? extends Element> elements = handler.doOperation(op, user, store);

        //The result size should be 1
        assertEquals(1, Iterables.size(elements));

        assertTrue(Iterables.contains(elements, expectedEntity1));
        elements.close();
    }

    private static void setupGraph(final AccumuloStore store) {
        List<Element> data = new ArrayList<>();

        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        final Entity entity = new Entity(TestGroups.ENTITY, "A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A" + i)
                    .directed(true)
                    .property(AccumuloPropertyNames.COUNT, 23)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build()
            );

            data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A" + i)
                    .directed(true)
                    .property(AccumuloPropertyNames.COUNT, 23)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .build()
            );

            data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A" + i)
                    .directed(true)
                    .property(AccumuloPropertyNames.COUNT, 23)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .build()
            );

            data.add(new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A" + i)
                    .property(AccumuloPropertyNames.COUNT, i)
                    .build());
        }
        addElements(data, store, new User());
    }


    private static void addElements(final Iterable<Element> data, final AccumuloStore store, final User user) {
        try {
            store.execute(new AddElements.Builder().input(data).build(), new Context(user));
        } catch (final OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}
