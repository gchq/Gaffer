/*
 * Copyright 2020-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.JcsCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreCacheTransient.FEDERATED_STORE_CACHE_SERVICE_NAME;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_MAP;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.MAP_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_2;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_USER_ID;

public class FederatedStoreCacheBackwardCompatibilityTest {

    private static final String ADDING_USER_ID = AUTH_USER_ID;
    private static final String BACKWARDS_COMPATABILITY_2_1_0 = "backwards_compatability_2.1.0";
    public static final String GAFFER_2_1_0_CACHE_CACHE_CCF = "src/test/resources/gaffer-2.1.0-cache/cache.ccf";
    private static FederatedStoreCache federatedStoreCache;

    @AfterAll
    public static void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @BeforeEach
    public void setUp() {
        resetForFederatedTests();

        Properties properties = new Properties();
        // Note that this config causes a binary resource file containing data to be loaded into the cache
        // This data includes MAP_ID_1 and user auths
        properties.setProperty(CacheProperties.CACHE_CONFIG_FILE, GAFFER_2_1_0_CACHE_CACHE_CCF);

        CacheServiceLoader.initialise(FEDERATED_STORE_CACHE_SERVICE_NAME, JcsCacheService.class.getName(), properties);
        federatedStoreCache = new FederatedStoreCache(BACKWARDS_COMPATABILITY_2_1_0);

        new Graph.Builder().config(new GraphConfig(GRAPH_ID_MAP))
                .addStoreProperties(loadStoreProperties(MAP_STORE_SINGLE_USE_PROPERTIES))
                .addSchema(loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON))
                .build();
    }

    @Test
    public void shouldReturnExpectedFederatedAccessUsingCacheDataFromVersion2() throws CacheOperationException {
        final Set<String> graphAuths = new HashSet<>(asList(AUTH_1, AUTH_2));

        final FederatedAccess access = new FederatedAccess(graphAuths, ADDING_USER_ID);

        final FederatedAccess accessFromCacheVersion2_1 = federatedStoreCache.getAccessFromCache(GRAPH_ID_MAP);

        assertEquals(access.getReadAccessPredicate(), accessFromCacheVersion2_1.getReadAccessPredicate());
        assertEquals(access.getWriteAccessPredicate(), accessFromCacheVersion2_1.getWriteAccessPredicate());
        assertEquals(access.getOrDefaultReadAccessPredicate(), accessFromCacheVersion2_1.getOrDefaultReadAccessPredicate());
        assertEquals(access.getOrDefaultWriteAccessPredicate(), accessFromCacheVersion2_1.getOrDefaultWriteAccessPredicate());
    }
}
