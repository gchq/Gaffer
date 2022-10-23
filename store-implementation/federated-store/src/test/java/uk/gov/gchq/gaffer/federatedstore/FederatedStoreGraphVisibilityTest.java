/*
 * Copyright 2017-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreGraphVisibilityTest {

    public static final Schema SCHEMA_BASIC_ENTITY = loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON);

    public static final Schema SCHEMA_BASIC_EDGE = loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON);
    private static final AccumuloProperties ACCUMULO_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    private static final User ADDING_USER = testUser();
    private static final User NON_ADDING_USER = blankUser();
    private static final User AUTH_NON_ADDING_USER = authUser();
    private Graph federatedGraph;

    @AfterAll
    public static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        federatedGraph = new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(federatedStoreProperties)
                .build();
    }

    @Test
    public void shouldNotShowHiddenGraphIdWithIDs() throws Exception {
        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_ENTITIES)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(SCHEMA_BASIC_ENTITY.clone())
                        .build(),
                ADDING_USER);

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_EDGES)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(SCHEMA_BASIC_EDGE.clone())
                        .graphAuths(AUTH_1)
                        .build(),
                ADDING_USER);


        commonAssertions();
    }

    /*
     * Adhoc test to make sure that the naming of props and schemas without ID's
     * is still retrievable via the name of the graph that is was added to the LIBRARY.
     */
    @Test
    public void shouldNotShowHiddenGraphIdWithoutIDs() throws Exception {

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_ENTITIES)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(SCHEMA_BASIC_ENTITY.clone())
                        .build(),
                ADDING_USER);

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_EDGES)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(SCHEMA_BASIC_EDGE.clone())
                        .graphAuths(AUTH_1)
                        .build(),
                ADDING_USER);


        commonAssertions();
    }

    private void commonAssertions() throws uk.gov.gchq.gaffer.operation.OperationException {
        assertThat(federatedGraph.execute(new GetAllGraphIds(), NON_ADDING_USER))
                .withFailMessage("Returned iterable should not be null, it should be empty.")
                .isNotNull()
                .withFailMessage("Showing hidden graphId")
                .isEmpty();

        assertThat(federatedGraph.execute(new GetAllGraphIds(), AUTH_NON_ADDING_USER))
                .withFailMessage("Returned iterable should not be null, it should be empty.")
                .isNotNull()
                .withFailMessage("Not Showing graphId with correct auth")
                .isNotEmpty()
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO_WITH_EDGES);

        assertThat(federatedGraph.execute(new GetAllGraphIds(), ADDING_USER))
                .withFailMessage("Returned iterable should not be null, it should be empty.")
                .isNotNull()
                .withFailMessage("Not Showing all graphId for adding user")
                .isNotEmpty()
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO_WITH_ENTITIES, GRAPH_ID_ACCUMULO_WITH_EDGES);

    }
}
