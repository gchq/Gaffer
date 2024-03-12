/*
 * Copyright 2017-2024 Crown Copyright
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
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
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
    public static final GraphLibrary LIBRARY = new HashMapGraphLibrary();
    private static final AccumuloProperties ACCUMULO_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    private static final User ADDING_USER = testUser();
    private static final User NON_ADDING_USER = blankUser();
    private static final User AUTH_NON_ADDING_USER = authUser();
    public static final String PROP_ID_FOR_LIB_ENTRY = "propIdForLibraryEntry";
    public static final String SCHEMA_ID_FOR_LIB_ENTRY = "schemaIDForLibraryEntry";
    public static final String GRAPH_ENTRY_IN_LIBRARY = "graphEntryInLibrary";
    public static final String UNUSED_ID = "unusedId";
    private Graph federatedGraph;

    @AfterAll
    public static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        federatedGraph = new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .library(LIBRARY)
                        .build())
                .addStoreProperties(federatedStoreProperties)
                .build();
    }

    @Test
    public void shouldNotShowHiddenGraphIdWithParentIDs() throws Exception {
        LIBRARY.add(UNUSED_ID, SCHEMA_ID_FOR_LIB_ENTRY, SCHEMA_BASIC_ENTITY.clone(), PROP_ID_FOR_LIB_ENTRY, ACCUMULO_PROPERTIES.clone());

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_ENTITIES)
                        .parentPropertiesId(PROP_ID_FOR_LIB_ENTRY) //<-- with properties Id from Library
                        .parentSchemaIds(singletonList(SCHEMA_ID_FOR_LIB_ENTRY)) //<-- with schema Id from Library
                        .build(),
                ADDING_USER);

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_EDGES)
                        .parentPropertiesId(PROP_ID_FOR_LIB_ENTRY) //<-- with properties Id from Library
                        .parentSchemaIds(singletonList(SCHEMA_ID_FOR_LIB_ENTRY)) //<-- with schema Id from Library
                        .graphAuths(AUTH_1)
                        .build(),
                ADDING_USER);


        commonAssertions();
    }

    /*
     * Adhoc test to make sure that the naming of props and schemas without ID's
     * is still retrievable via the name of the graph that was added to the LIBRARY.
     */
    @Test
    public void shouldNotShowHiddenGraphIdWithParentIdsOfAGraph() throws Exception {

        LIBRARY.add(GRAPH_ENTRY_IN_LIBRARY, SCHEMA_BASIC_ENTITY.clone(), ACCUMULO_PROPERTIES.clone());

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_ENTITIES)
                        .parentPropertiesId(GRAPH_ENTRY_IN_LIBRARY) //<-- get same properties used by this graph in the library
                        .parentSchemaIds(singletonList(GRAPH_ENTRY_IN_LIBRARY)) //<-- get same schema used by this graph in the library
                        .build(),
                ADDING_USER);

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_EDGES)
                        .parentPropertiesId(GRAPH_ENTRY_IN_LIBRARY) //<-- get same properties used by this graph in the library
                        .parentSchemaIds(singletonList(GRAPH_ENTRY_IN_LIBRARY)) //<-- get same schema used by this graph in the library
                        .graphAuths(AUTH_1)
                        .build(),
                ADDING_USER);


        commonAssertions();
    }
 @Test
    public void shouldNotShowHiddenGraphIdWithParentIdsOfAGraphWithUnusedId() throws Exception {

     LIBRARY.add(GRAPH_ENTRY_IN_LIBRARY, UNUSED_ID, SCHEMA_BASIC_ENTITY.clone(), UNUSED_ID, ACCUMULO_PROPERTIES.clone());

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_ENTITIES)
                        .parentPropertiesId(GRAPH_ENTRY_IN_LIBRARY) //<-- get same properties used by this graph in the library, without using the property id
                        .parentSchemaIds(singletonList(GRAPH_ENTRY_IN_LIBRARY)) //<-- get same schema used by this graph in the library, without using the schema id
                        .build(),
                ADDING_USER);

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO_WITH_EDGES)
                        .parentPropertiesId(GRAPH_ENTRY_IN_LIBRARY) //<-- get same properties used by this graph in the library, without using the property id
                        .parentSchemaIds(singletonList(GRAPH_ENTRY_IN_LIBRARY)) //<-- get same schema used by this graph in the library, without using the schema id
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
