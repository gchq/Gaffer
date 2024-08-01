/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.commonutil.TestPropertyNames.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.store.TestTypes.BOOLEAN_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.store.TestTypes.STRING_TYPE;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

class FederatedStoreGetElementsWithinSetTest {
    private static final AccumuloProperties ACCUMULO_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreGetElementsWithinSetTest.class, "properties/singleUseAccumuloStore.properties"));
    private static final User USER = new User();
    private static final Set<EntityId> SEEDS = new LinkedHashSet<>();
    private static final String GRAPH_IDS = String.format("%s,%s", GRAPH_ID_A, GRAPH_ID_B);
    private static final View EDGE_VIEW = new View.Builder().edge(GROUP_BASIC_EDGE).build();
    private static final View ENTITY_VIEW = new View.Builder().entity(GROUP_BASIC_ENTITY).build();

    private Graph federatedGraph;

    @AfterAll
    static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    void setUp() throws Exception {
        resetForFederatedTests();
        FederatedStoreProperties federatedStoreProperties = getFederatedStorePropertiesWithHashMapCache();
        for (int i = 0; i < 300; i++) {
            SEEDS.add(new EntitySeed("A" + i));
        }

        federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(federatedStoreProperties)
                .build();

        federatedGraph.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_A)
                .storeProperties(ACCUMULO_PROPERTIES)
                .schema(getSchema())
                .build(), USER);

        federatedGraph.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_B)
                .storeProperties(ACCUMULO_PROPERTIES)
                .schema(getSchema())
                .build(), USER);

        for (int i = 300; i >= 0; i--) {
            addEntities("A" + i, GRAPH_ID_A);
            addEntities("A" + i, GRAPH_ID_B);
        }

        // Add 6 edges total - should all have src and dest in different batches
        addEdges("A244", "A87", GRAPH_ID_A);
        addEdges("A168", "A110", GRAPH_ID_A);
        addEdges("A56", "A299", GRAPH_ID_A);
        addEdges("A297", "A193", GRAPH_ID_B);
        addEdges("A15", "A285", GRAPH_ID_B);
        addEdges("A1", "A52", GRAPH_ID_B);
    }

    @Test
    void shouldReturnOnlyEdgesWhenViewContainsNoEntities() throws Exception {
        // Given/When
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedGraph.execute(new FederatedOperation.Builder()
                .op(new GetElementsWithinSet.Builder()
                            .view(EDGE_VIEW)
                            .input(SEEDS)
                        .build())
                .graphIdsCSV(GRAPH_IDS)
                .build(), USER);

        // Then
        assertThat(results)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .containsAll(getExpectedEdges());
    }

    @Test
    void shouldReturnOnlyEntitiesWhenViewContainsNoEdges() throws Exception {
        // Given/When
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedGraph.execute(new FederatedOperation.Builder()
                .op(new GetElementsWithinSet.Builder()
                            .view(ENTITY_VIEW)
                            .input(SEEDS)
                        .build())
                .graphIdsCSV(GRAPH_IDS)
                .build(), USER);

        // Then
        assertThat(results).hasSize(600);
        assertThat(results).extracting(r -> r.getGroup()).contains(GROUP_BASIC_ENTITY);
    }

    @Test
    void shouldGetAllEdgesWithSmallerBatchSizeInAFederatedOperation() throws Exception {
        // Given
        // Set batch scanner entries to 50 - so some edges will have its src in the final batch but its
        // dest in the first - should all still be retrieved
        ACCUMULO_PROPERTIES.setMaxEntriesForBatchScanner("50");

        // When
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedGraph.execute(new FederatedOperation.Builder()
                .op(new GetElementsWithinSet.Builder()
                            .view(EDGE_VIEW)
                            .input(SEEDS)
                        .build())
                .graphIdsCSV(GRAPH_IDS)
                .build(), USER);

        // Then
        assertThat(results)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .containsAll(getExpectedEdges());
    }

    @Test
    void shouldGetAllEdgesWithSmallerBatchSizeStandardOperation() throws Exception {
        // Given
        // Set batch scanner entries to 50 - so some edges will have its src in the final batch but its
        // dest in the first - should all still be retrieved
        ACCUMULO_PROPERTIES.setMaxEntriesForBatchScanner("50");

        // When
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(EDGE_VIEW)
                .input(SEEDS)
                .build();
        final Iterable<? extends Element> results = federatedGraph.execute(op, USER);

        // Then
        assertThat(results)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .containsAll(getExpectedEdges());
    }

    private void addEntities(final String source, final String graphId) throws OperationException {
        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(new Entity.Builder()
                            .group(GROUP_BASIC_ENTITY)
                            .vertex(source)
                            .build())
                        .build())
                .graphIdsCSV(graphId)
                .build(), USER);
    }

    private void addEdges(final String source, final String dest, final String graphId) throws OperationException {
        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(new Edge.Builder()
                            .group(GROUP_BASIC_EDGE)
                            .source(source)
                            .dest(dest)
                            .directed(true)
                            .build())
                        .build())
                .graphIdsCSV(graphId)
                .build(), USER);
    }

    private Set<Element> getExpectedEdges() {
        final Set<Element> expectedEdges = new HashSet<>();
        expectedEdges.add(new Edge.Builder()
            .group(GROUP_BASIC_EDGE)
            .source("A244")
            .dest("A87")
            .directed(true)
            .build());
        expectedEdges.add(new Edge.Builder()
            .group(GROUP_BASIC_EDGE)
            .source("A168")
            .dest("A110")
            .directed(true)
            .build());
        expectedEdges.add(new Edge.Builder()
            .group(GROUP_BASIC_EDGE)
            .source("A56")
            .dest("A299")
            .directed(true)
            .build());
        expectedEdges.add(new Edge.Builder()
            .group(GROUP_BASIC_EDGE)
            .source("A297")
            .dest("A193")
            .directed(true)
            .build());
        expectedEdges.add(new Edge.Builder()
            .group(GROUP_BASIC_EDGE)
            .source("A15")
            .dest("A285")
            .directed(true)
            .build());
        expectedEdges.add(new Edge.Builder()
            .group(GROUP_BASIC_EDGE)
            .source("A1")
            .dest("A52")
            .directed(true)
            .build());
        return expectedEdges;
    }

    private Schema getSchema() {
        return new Schema.Builder()
                .entity(GROUP_BASIC_ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(STRING)
                        .build())
                .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .directed(DIRECTED_EITHER)
                        .build())
                .type(STRING, STRING_TYPE)
                .type(DIRECTED_EITHER, BOOLEAN_TYPE)
                .build();
    }
}
