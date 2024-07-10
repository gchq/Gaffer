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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
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
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.MAP_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.store.TestTypes.BOOLEAN_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.store.TestTypes.STRING_TYPE;

public class FederatedDeleteElementsTest {
    private static final StoreProperties PROPERTIES_MAP_STORE = loadStoreProperties(MAP_STORE_SINGLE_USE_PROPERTIES);
    private static final GetAllElements GET_ALL_ELEMENTS = new GetAllElements.Builder().build();
    private static final User USER = new User();
    private static Graph federatedGraph;
    private static Graph federatedGraphMapStore;


    @AfterAll
    public static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        federatedGraphMapStore = getFederatedGraph(PROPERTIES_MAP_STORE);
    }

    @Test
    void shouldDeleteEntityFromSingleGraphWithMapStore() throws Exception {
        // When
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("1"))
                        .view(new View.Builder().entity(GROUP_BASIC_ENTITY).build())
                        .build())
                .then(new DeleteElements())
                .build();
        federatedGraphMapStore.execute(getFederatedOperation(chain).graphIdsCSV("graphA"), USER);

        // Then
        // Vertex 1 deleted - edge 1->2 remains
        final Iterable<? extends Element> results = federatedGraphMapStore.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(8)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("1"));
    }

    @Test
    void shouldDeleteEdgeFromSingleGraphWithMapStore() throws Exception {
        // When
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EdgeSeed("1", "2"))
                        .view(new View.Builder().edge(GROUP_BASIC_EDGE).build())
                        .build())
                .then(new DeleteElements())
                .build();
        federatedGraphMapStore.execute(getFederatedOperation(chain).graphIdsCSV("graphA"), USER);

        // Then
        // Edge 1->2 deleted
        final Iterable<? extends Element> results = federatedGraphMapStore.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(8)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEdge("1", "2"));
    }

    @Test
    void shouldDeleteEntityAndEdgesFromSingleGraphhWithMapStore() throws Exception {
        // Given/When
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("2"))
                        .build())
                .then(new DeleteElements())
                .build();
        federatedGraphMapStore.execute(getFederatedOperation(chain).graphIdsCSV("graphA"), USER);

        // Then
        // Vertex 2 deleted from graph A - removing 1->2 as well
        final Iterable<? extends Element> results = federatedGraphMapStore.execute(
            getFederatedOperation(GET_ALL_ELEMENTS).graphIdsCSV("graphA"), USER);
        assertThat(results)
            .hasSize(2)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("2"), getEdge("1", "2"));
    }

    @Test
    void shouldDeleteEntityFromBothGraphsWithMapStore() throws Exception {
        // When
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("1"))
                        .view(new View.Builder().entity(GROUP_BASIC_ENTITY).build())
                        .build())
                .then(new DeleteElements())
                .build();
        federatedGraphMapStore.execute(getFederatedOperation(chain), USER);

        // Then
        // Vertex 1 deleted, edge 1->2 remains
        final Iterable<? extends Element> results = federatedGraphMapStore.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(8)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("1"));
    }

    @Test
    void shouldDeleteEdgeFromBothGraphsWithMapStore() throws Exception {
        // When
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EdgeSeed("1", "2"))
                        .view(new View.Builder().edge(GROUP_BASIC_EDGE).build())
                        .build())
                .then(new DeleteElements())
                .build();
        federatedGraphMapStore.execute(getFederatedOperation(chain), USER);

        // Then
        // Edge 1->2 deleted - Both vertices should remain
        final Iterable<? extends Element> results = federatedGraphMapStore.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(8)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEdge("1", "2"));
    }

    @Test
    void shouldDeleteEntityAndEdgesFromBothGraphshWithMapStore() throws Exception {
        // Given/When
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("3"))
                        .build())
                .then(new DeleteElements())
                .build();
        federatedGraphMapStore.execute(getFederatedOperation(chain), USER);

        // Then
        // Vertex 3 deleted from both graphs - will remove 2->3 and 3->4
        final Iterable<? extends Element> results = federatedGraphMapStore.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(6)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEdge("2", "3"), getEdge("3", "4"), getEntity("3"));
    }

    static Graph getFederatedGraph(StoreProperties storeProperties) throws OperationException {
        FederatedStoreProperties federatedStoreProperties = getFederatedStorePropertiesWithHashMapCache();

        federatedGraph = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .build())
            .addStoreProperties(federatedStoreProperties)
            .build();

        federatedGraph.execute(new AddGraph.Builder()
            .graphId(GRAPH_ID_A)
            .storeProperties(storeProperties)
            .schema(getSchema())
            .build(), USER);

        federatedGraph.execute(new AddGraph.Builder()
            .graphId(GRAPH_ID_B)
            .storeProperties(storeProperties)
            .schema(getSchema())
            .build(), USER);

        // 1 -> 2 in Graph A
        // 2 -> 3 in Graph A
        addElements("1", "2", GRAPH_ID_A);
        addElements("2", "3", GRAPH_ID_A);
        // 3 -> 4 in Graph B
        // 4 -> 5 in Graph B
        addElements("3", "4", GRAPH_ID_B);
        addElements("4", "5", GRAPH_ID_B);

        return federatedGraph;
    }

    private static void addElements(final String source, final String dest, final String graphId) throws OperationException {
        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(getEntity(source), getEntity(dest), getEdge(source, dest))
                        .build())
                .graphIdsCSV(graphId)
                .build(), USER);
    }

    private static Edge getEdge(final String source, final String dest) {
        return new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(source)
                .dest(dest)
                .directed(true)
                .build();
    }

    private static Entity getEntity(final String vertex) {
        return new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex(vertex)
                .build();
    }

    private static Schema getSchema() {
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
