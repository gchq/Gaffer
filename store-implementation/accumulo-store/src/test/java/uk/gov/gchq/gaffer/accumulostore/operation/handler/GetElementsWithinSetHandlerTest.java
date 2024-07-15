/*
 * Copyright 2016-2024 Crown Copyright
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

import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.TableExistsException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class GetElementsWithinSetHandlerTest {

    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(GetElementsWithinSetHandlerTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(GetElementsWithinSetHandlerTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(GetElementsWithinSetHandlerTest.class, "/accumuloStoreClassicKeys.properties"));
    private static final AccumuloStore BYTE_ENTITY_STORE = new SingleUseMiniAccumuloStore();
    private static final AccumuloStore GAFFER_1_KEY_STORE = new SingleUseMiniAccumuloStore();

    private static View defaultView;

    private static Edge expectedEdge1 = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A0")
            .dest("A23")
            .directed(true)
            .matchedVertex(EdgeId.MatchedVertex.SOURCE)
            .build();
    private static Edge expectedEdge2 = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A0")
            .dest("A23")
            .directed(true)
            .matchedVertex(EdgeId.MatchedVertex.SOURCE)
            .build();
    private static Edge expectedEdge3 = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A0")
            .dest("A23")
            .directed(true)
            .matchedVertex(EdgeId.MatchedVertex.SOURCE)
            .build();
    private static Entity expectedEntity1 = new Entity.Builder()
            .group(TestGroups.ENTITY)
            .vertex("A0")
            .build();
    private static Entity expectedEntity2 = new Entity.Builder()
            .group(TestGroups.ENTITY)
            .vertex("A23")
            .build();
    private static Edge expectedSummarisedEdge = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A0")
            .dest("A23")
            .directed(true)
            .build();
    private static Edge expectedSummarisedEdgePropertiesEmptySet = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A0")
            .dest("A23")
            .directed(true)
            .matchedVertex(EdgeId.MatchedVertex.SOURCE)
            .build();
    private static Edge expectedSummarisedEdgePropertiesFiltered = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A0")
            .dest("A23")
            .directed(true)
            .matchedVertex(EdgeId.MatchedVertex.SOURCE)
            .property(AccumuloPropertyNames.COUNT, 23 * 3)
            .build();

    final Set<EntityId> seeds = new HashSet<>(Arrays.asList(new EntitySeed("A0"), new EntitySeed("A23")));

    private final User user = new User();

    @BeforeEach
    public void reInitialise() throws StoreException, OperationException, TableExistsException {
        expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        expectedEdge1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedEdge1.putProperty(AccumuloPropertyNames.PROP_4, 0);

        expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
        expectedEdge2.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedEdge2.putProperty(AccumuloPropertyNames.PROP_4, 0);

        expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
        expectedEdge3.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedEdge3.putProperty(AccumuloPropertyNames.PROP_4, 0);

        expectedEntity1.putProperty(AccumuloPropertyNames.COUNT, 10000);

        expectedEntity2.putProperty(AccumuloPropertyNames.COUNT, 23);

        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 6);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.COUNT, 23 * 3);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_1, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_2, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_3, 0);
        expectedSummarisedEdge.putProperty(AccumuloPropertyNames.PROP_4, 0);

        defaultView = new View.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .build();

        BYTE_ENTITY_STORE.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        GAFFER_1_KEY_STORE.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        setupGraph(BYTE_ENTITY_STORE);
        setupGraph(GAFFER_1_KEY_STORE);
    }

    @Test
    void shouldReturnElementsNoSummarisationByteEntityStore() throws OperationException {
        shouldReturnElementsNoSummarisation(BYTE_ENTITY_STORE);
    }

    @Test
    void shouldReturnElementsNoSummarisationGaffer1Store() throws OperationException {
        shouldReturnElementsNoSummarisation(GAFFER_1_KEY_STORE);
    }

    private void shouldReturnElementsNoSummarisation(final AccumuloStore store) throws OperationException {
        final GetElementsWithinSet operation = new GetElementsWithinSet.Builder().view(defaultView).input(seeds)
                .build();
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();

        final Iterable<? extends Element> elements = handler.doOperation(operation, user, store);
        // Without query compaction the result size should be 5
        final Set<Element> elementSet = Sets.newHashSet(elements);
        assertThat(elementSet).hasSize(5);
        assertThat(elementSet).containsOnly(expectedEdge1, expectedEdge2, expectedEdge3, expectedEntity1, expectedEntity2);
        for (final Element element : elementSet) {
            if (element instanceof Edge) {
                assertThat(((Edge) element).getMatchedVertex()).isEqualTo(EdgeId.MatchedVertex.SOURCE);
            }
        }
    }

    @Test
    void shouldSummariseByteEntityStore() throws OperationException {
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        runTest(BYTE_ENTITY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdge, expectedEntity1, expectedEntity2);
    }

    @Test
    void shouldSummariseGaffer1Store() throws OperationException {
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        runTest(GAFFER_1_KEY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdge, expectedEntity1, expectedEntity2);
    }

    @Test
    void shouldReturnOnlyEdgesWhenViewContainsNoEntitiesByteEntityStore() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        runTest(BYTE_ENTITY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdge);
    }

    @Test
    void shouldReturnOnlyEdgesWhenViewContainsNoEntitiesGaffer1Store() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        runTest(GAFFER_1_KEY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdge);
    }

    @Test
    void shouldReturnOnlyEdgesWhenViewContainsNoEntitiesPropertiesFilteredByteEntityStore() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(Collections.singleton(AccumuloPropertyNames.COUNT))
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .properties(Collections.singleton(AccumuloPropertyNames.COUNT))
                        .groupBy()
                        .build())
                .build();

        runTest(BYTE_ENTITY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdgePropertiesFiltered);
    }

    @Test
    void shouldReturnOnlyEdgesWhenViewContainsNoEntitiesPropertiesFilteredGaffer1Store() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(Collections.singleton(AccumuloPropertyNames.COUNT))
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .properties(Collections.singleton(AccumuloPropertyNames.COUNT))
                        .groupBy()
                        .build())
                .build();

        runTest(GAFFER_1_KEY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdgePropertiesFiltered);
    }

    @Test
    void shouldReturnOnlyEdgesWhenViewContainsNoEntitiesPropertiesEmptySetByteEntityStore() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .build();

        runTest(BYTE_ENTITY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdgePropertiesEmptySet);
    }

    @Test
    void shouldReturnOnlyEdgesWhenViewContainsNoEntitiesPropertiesEmptySetGaffer1Store() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .build();

        runTest(GAFFER_1_KEY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedSummarisedEdgePropertiesEmptySet);
    }

    @Test
    void shouldReturnOnlyEntitiesWhenViewContainsNoEdgesByteEntityStore() throws OperationException {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        runTest(BYTE_ENTITY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedEntity1, expectedEntity2);
    }

    @Test
    void shouldReturnOnlyEntitiesWhenViewContainsNoEdgesGaffer1Store() throws OperationException {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        runTest(GAFFER_1_KEY_STORE, view, IncludeIncomingOutgoingType.OUTGOING, expectedEntity1, expectedEntity2);
    }

    @Test
    void shouldGetIncomingEdgesOnlyGaffer1Store() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .build();

        // Incoming edge should have matched vertex DESTINATION
        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("A0")
                .dest("A23")
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                .build();

        runTest(GAFFER_1_KEY_STORE, view, IncludeIncomingOutgoingType.INCOMING, expectedEdge);
    }

    @Test
    void shouldGetIncomingEdgesOnlyByteEntityStore() throws OperationException {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .build();

        // Incoming edge should have matched vertex DESTINATION
        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("A0")
                .dest("A23")
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                .build();

        runTest(BYTE_ENTITY_STORE, view, IncludeIncomingOutgoingType.INCOMING, expectedEdge);
    }

    @Test
    void shouldGetEdgesWhenDestAndSourceAreInDifferentBatchesGaffer1Store() throws OperationException {
        // Given
        // Set batch scanner entries to 20 - so edge A99 -> A1 will have its src in the final batch but its
        // dest in the first
        GAFFER_1_KEY_STORE.getProperties().setMaxEntriesForBatchScanner("20");
        final Set<EntityId> seedSet = new HashSet<>();
        for (int i = 1; i < 100; i++) {
            seedSet.add(new EntitySeed("A" + i));
        }

        // When
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(new View.Builder()
                    .edge(TestGroups.EDGE,  new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .build())
                .input(seedSet)
                .build();
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();

        final Iterable<? extends Element> results = handler.doOperation(op, user, GAFFER_1_KEY_STORE);

        // Then
        final Edge expectedEdge = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A99")
            .dest("A1")
            .directed(true)
            .build();

        assertThat(results)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .contains(expectedEdge);
    }

    @Test
    void shouldGetEdgesWhenDestAndSourceAreInDifferentBatchesByteEntityStore() throws OperationException {
        // Given
        // Set batch scanner entries to 20 - so edge A99 -> A1 will have its src in the final batch but its
        // dest in the first
        BYTE_ENTITY_STORE.getProperties().setMaxEntriesForBatchScanner("20");
        final Set<EntityId> seedSet = new HashSet<>();
        for (int i = 1; i < 100; i++) {
            seedSet.add(new EntitySeed("A" + i));
        }

        // When
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(new View.Builder()
                    .edge(TestGroups.EDGE,  new ViewElementDefinition.Builder()
                        .properties(Collections.emptySet())
                        .groupBy()
                        .build())
                .build())
                .input(seedSet)
                .build();
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();

        final Iterable<? extends Element> results = handler.doOperation(op, user, GAFFER_1_KEY_STORE);

        // Then
        final Edge expectedEdge = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source("A99")
            .dest("A1")
            .directed(true)
            .build();

        assertThat(results)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .contains(expectedEdge);
    }

    private void runTest(final AccumuloStore store, final View view, final IncludeIncomingOutgoingType type, final Element... expectedElements) throws OperationException {
        final GetElementsWithinSet operation = new GetElementsWithinSet.Builder().view(view).input(seeds).build();
        operation.setIncludeIncomingOutGoing(type);
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();

        final Iterable<? extends Element> elements = handler.doOperation(operation, user, store);
        // After query compaction the result size should be 1
        assertThat(elements)
                .hasSize(expectedElements.length)
                .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
                .containsOnly(expectedElements);
    }

    private static void setupGraph(final AccumuloStore store) throws OperationException, StoreException, TableExistsException {
        // Create table
        // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
        // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
        TableUtils.createTable(store);

        final List<Element> data = new ArrayList<>();
        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A" + i)
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COUNT, i)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build());

            data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A" + i)
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.COUNT, i)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build());

            data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A" + i)
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COUNT, i)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build());

            data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A99")
                    .dest("A1")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());

            data.add(new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A" + i)
                    .property(AccumuloPropertyNames.COUNT, i)
                    .build());
        }
        final User user = new User();
        addElements(data, user, store);
    }

    private static void addElements(final Iterable<Element> data, final User user, final AccumuloStore store) throws OperationException {
        store.execute(new AddElements.Builder().input(data).build(), new Context(user));
    }
}
