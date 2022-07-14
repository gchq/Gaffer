/*
 * Copyright 2020-2022 Crown Copyright
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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_USER_ID;

public class AdminGetAllGraphInfoTest {

    private static final String ADMIN_AUTH = "AdminAuth";
    private static final User ADMIN_USER = new User("adminUser", null, Sets.newHashSet(ADMIN_AUTH));
    private static final AccumuloProperties PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    private FederatedAccess access;
    private FederatedStore store;

    @AfterAll
    public static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        access = new FederatedAccess(Sets.newHashSet(AUTH_1), AUTH_USER_ID, false, FederatedGraphStorage.DEFAULT_DISABLED_BY_DEFAULT);
        store = new FederatedStore();
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.set(StoreProperties.ADMIN_AUTH, ADMIN_AUTH);
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, storeProperties);
    }

    @Test
    public void shouldGetAllGraphsAndAuthsAsAdmin() throws Exception {
        store.addGraphs(access, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_ACCUMULO)
                        .build())
                .schema(new Schema())
                .properties(PROPERTIES)
                .build());

        final Map<String, Object> allGraphsAndAuths = store.getAllGraphsAndAuths(ADMIN_USER, null, true);


        assertThat(allGraphsAndAuths)
                .isNotNull()
                .size().isEqualTo(1);

        assertEquals("{\n" +
                "  \"AccumuloStore\" : {\n" +
                "    \"addingUserId\" : \"authUser\",\n" +
                "    \"disabledByDefault\" : false,\n" +
                "    \"graphAuths\" : [ \"auth1\" ],\n" +
                "    \"public\" : false\n" +
                "  }\n" +
                "}", new String(JSONSerialiser.serialise(allGraphsAndAuths, true)));
    }

    @Test
    public void shouldNotGetAllGraphsAndAuthsAsAdmin() throws Exception {
        store.addGraphs(access, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_ACCUMULO)
                        .build())
                .schema(new Schema())
                .properties(PROPERTIES)
                .build());

        final Map<String, Object> allGraphsAndAuths = store.getAllGraphsAndAuths(new User(), null, true);

        assertThat(allGraphsAndAuths)
                .isNotNull()
                .isEmpty();
    }
}
