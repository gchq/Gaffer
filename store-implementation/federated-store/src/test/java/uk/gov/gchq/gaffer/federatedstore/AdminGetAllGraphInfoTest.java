/*
 * Copyright 2020-2021 Crown Copyright
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
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AdminGetAllGraphInfoTest {

    private static final String ADMIN_AUTH = "AdminAuth";
    private static final User ADMIN_USER = new User("adminUser", null, Sets.newHashSet(ADMIN_AUTH));
    private static final AccumuloProperties PROPERTIES =
            AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AdminGetAllGraphInfoTest.class, "properties/singleUseAccumuloStore.properties"));

    private FederatedAccess access;
    private FederatedStore store;

    @BeforeEach
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        access = new FederatedAccess(Sets.newHashSet("authA"), "testuser1", false, FederatedGraphStorage.DEFAULT_DISABLED_BY_DEFAULT);
        store = new FederatedStore();
        final StoreProperties fedProps = new StoreProperties();
        fedProps.set(StoreProperties.ADMIN_AUTH, ADMIN_AUTH);
        store.initialise("testFedStore", null, fedProps);
        store.remove("graph1", ADMIN_USER, true);
    }

    @AfterAll
    public static void tearDownCache() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldGetAllGraphsAndAuthsAsAdmin() throws Exception {
        final String graph1 = "graph1";

        System.out.println(store.getAllGraphIds(ADMIN_USER, true));
        store.addGraphs(access, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graph1)
                        .build())
                .schema(new Schema())
                .properties(PROPERTIES)
                .build());

        final Map<String, Object> allGraphsAndAuths = store.getAllGraphsAndAuths(ADMIN_USER, null, true);

        assertNotNull(allGraphsAndAuths);
        assertFalse(allGraphsAndAuths.isEmpty());
        assertEquals(graph1, allGraphsAndAuths.keySet().toArray(new String[]{})[0]);
        assertEquals("{\n" +
                "  \"graph1\" : {\n" +
                "    \"addingUserId\" : \"testuser1\",\n" +
                "    \"disabledByDefault\" : false,\n" +
                "    \"graphAuths\" : [ \"authA\" ],\n" +
                "    \"public\" : false\n" +
                "  }\n" +
                "}", new String(JSONSerialiser.serialise(allGraphsAndAuths, true)));
    }

    @Test
    public void shouldNotGetAllGraphsAndAuthsAsAdmin() throws Exception {
        final String graph1 = "graph1";

        System.out.println(store.getAllGraphIds(ADMIN_USER, true));
        store.addGraphs(access, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graph1)
                        .build())
                .schema(new Schema())
                .properties(PROPERTIES)
                .build());

        final Map<String, Object> allGraphsAndAuths = store.getAllGraphsAndAuths(new User(), null, true);

        assertNotNull(allGraphsAndAuths);
        assertThat(allGraphsAndAuths).isEmpty();
    }
}
