/*
 * Copyright 2023-2024 Crown Copyright
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
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
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.DEPRECATED_GRAPHIDS_OPTION;
import static uk.gov.gchq.gaffer.store.TestTypes.BOOLEAN_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.store.TestTypes.STRING_TYPE;

public class FederatedStoreGetWalksTest {
    private static final StoreProperties PROPERTIES_MAP_STORE = loadStoreProperties(MAP_STORE_SINGLE_USE_PROPERTIES);
    private static final User USER = new User();
    private static final View VIEW = new View.Builder()
            .edge(GROUP_BASIC_EDGE)
            .entity(GROUP_BASIC_ENTITY)
            .build();
    public static final String VERTEX1 = "vertex1";
    public static final String VERTEX2 = "vertex2";
    public static final String VERTEX3 = "vertex3";
    private Graph federatedGraph;

    @AfterAll
    public static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        FederatedStoreProperties federatedStoreProperties = getFederatedStorePropertiesWithHashMapCache();

        federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(federatedStoreProperties)
                .build();

        federatedGraph.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_A)
                .storeProperties(PROPERTIES_MAP_STORE)
                .schema(getSchema())
                .build(), USER);

        federatedGraph.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_B)
                .storeProperties(PROPERTIES_MAP_STORE)
                .schema(getSchema())
                .build(), USER);

        addElements(VERTEX1, VERTEX2, GRAPH_ID_A);
        addElements(VERTEX2, VERTEX3, GRAPH_ID_B);
    }

    @Test
    public void shouldGetWalksAcrossSubGraphsWithFederatedOperation() throws Exception {
        // When
        Iterable<Walk> results = federatedGraph.execute(
                new GetWalks.Builder()
                        .input(VERTEX1)
                        .addOperations(new FederatedOperation.Builder()
                                .op(new GetElements.Builder().view(VIEW).build())
                                .graphIdsCSV(GRAPH_ID_A)
                                .build())
                        .addOperations(new FederatedOperation.Builder()
                                .op(new GetElements.Builder().view(VIEW).build())
                                .graphIdsCSV(GRAPH_ID_B)
                                .build())
                        .build(), USER);

        // Then
        assertThat(results)
                .containsExactly(new Walk.Builder()
                        .entity(getEntity(VERTEX1))
                        .edge(getEdge(VERTEX1, VERTEX2))
                        .entity(getEntity(VERTEX2))
                        .edge(getEdge(VERTEX2, VERTEX3))
                        .build());
    }

    @Test
    public void shouldGetWalksAcrossSubGraphsWithDeprecatedGraphIdsOption() throws Exception {
        // When
        Iterable<Walk> results = federatedGraph.execute(
                new GetWalks.Builder()
                        .input(VERTEX1)
                        .addOperations(new GetElements.Builder().view(VIEW).option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A).build())
                        .addOperations(new GetElements.Builder().view(VIEW).option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_B).build())
                        .build(), USER);

        // Then
        assertThat(results)
                .containsExactly(new Walk.Builder()
                        .entity(getEntity(VERTEX1))
                        .edge(getEdge(VERTEX1, VERTEX2))
                        .entity(getEntity(VERTEX2))
                        .edge(getEdge(VERTEX2, VERTEX3))
                        .build());
    }

    private void addElements(final String source, final String dest, final String graphId) throws OperationException {
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
