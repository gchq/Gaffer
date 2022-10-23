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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreMultiCacheTest {

    public static final User AUTH_USER = authUser();
    public static final User TEST_USER = testUser();
    public static final User BLANK_USER = blankUser();
    private static final AccumuloProperties ACCUMULO_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    public FederatedStore federatedStore;
    public FederatedStore federatedStore2WithSameCache;
    public FederatedStoreProperties federatedStoreProperties;

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        federatedStoreProperties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));
        federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedStoreProperties);
        federatedStore.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_ACCUMULO)
                .graphAuths(AUTH_1)
                .isPublic(false)
                .storeProperties(ACCUMULO_PROPERTIES.clone())
                .schema(loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON))
                .build(), new Context.Builder()
                .user(TEST_USER)
                .build());


        federatedStore2WithSameCache = new FederatedStore();
        federatedStore2WithSameCache.initialise(GRAPH_ID_TEST_FEDERATED_STORE + 2, null, federatedStoreProperties);
    }

    @AfterEach
    public void after() {
        resetForFederatedTests();
    }

    @Test
    public void shouldInitialiseByCacheToContainSameGraphsForAddingUser() throws Exception {
        final Collection<String> fed1TestUserGraphs = federatedStore.getAllGraphIds(TEST_USER);
        Collection<String> fed2WithSameCacheTestUserGraphs = federatedStore2WithSameCache.getAllGraphIds(TEST_USER);

        assertThat(fed1TestUserGraphs)
                .withFailMessage("adding user should have visibility of first store graphs")
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO);

        assertThat(fed2WithSameCacheTestUserGraphs)
                .withFailMessage("adding user should have same visibility of second store graphs")
                .containsExactlyInAnyOrderElementsOf(fed1TestUserGraphs);
    }

    @Test
    public void shouldInitialiseByCacheToContainSameGraphsForAuthUser() throws Exception {
        final Collection<String> fed1AuthUserGraphs = federatedStore.getAllGraphIds(AUTH_USER);
        Collection<String> fed2WithSameCacheAuthUserGraphs = federatedStore2WithSameCache.getAllGraphIds(AUTH_USER);

        assertThat(fed1AuthUserGraphs)
                .withFailMessage("auth user should have visibility of first store graphs")
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO);

        assertThat(fed2WithSameCacheAuthUserGraphs)
                .withFailMessage("auth user should have same visibility of second store graphs")
                .containsExactlyInAnyOrderElementsOf(fed1AuthUserGraphs);
    }

    @Test
    public void shouldInitialiseByCacheToContainSameGraphsForBlankUser() throws Exception {
        final Collection<String> fed1BlankUserGraphs = federatedStore.getAllGraphIds(BLANK_USER);

        assertThat(fed1BlankUserGraphs)
                .withFailMessage("blank user should not have visibility of first store graphs")
                .isEmpty();

        Collection<String> fed2WithSameCacheBlankUserGraphs = federatedStore2WithSameCache.getAllGraphIds(BLANK_USER);
        assertThat(fed2WithSameCacheBlankUserGraphs)
                .withFailMessage("blank user should have same visibility of second store graphs")
                .containsExactlyInAnyOrderElementsOf(fed1BlankUserGraphs);
    }


    @Test
    public void shouldInitialiseByCacheToContainSamePublicGraphsForBlankUser() throws Exception {
        federatedStore.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_ACCUMULO + 2)
                .isPublic(true)
                .storeProperties(ACCUMULO_PROPERTIES.clone())
                .schema(loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON))
                .build(), new Context.Builder()
                .user(TEST_USER)
                .build());

        federatedStore2WithSameCache = new FederatedStore();
        federatedStore2WithSameCache.initialise(GRAPH_ID_TEST_FEDERATED_STORE + 2, null, federatedStoreProperties);

        assertThat(federatedStore.getAllGraphIds(TEST_USER))
                .withFailMessage("There should be 2 graphs")
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO, GRAPH_ID_ACCUMULO + 2);

        assertThat(federatedStore2WithSameCache.getAllGraphIds(TEST_USER))
                .withFailMessage("There should be 2 graphs")
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO, GRAPH_ID_ACCUMULO + 2);

        assertThat(federatedStore.getAllGraphIds(BLANK_USER))
                .withFailMessage("blank user should have visibility of public graph")
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO + 2);

        assertThat(federatedStore2WithSameCache.getAllGraphIds(BLANK_USER))
                .withFailMessage("blank user should have same visibility of second store graphs")
                .containsExactlyInAnyOrder(GRAPH_ID_ACCUMULO + 2);
    }
}
