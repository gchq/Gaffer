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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.JcsCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStoreCacheBackwardCompatibilityTest {

    //TODO fs test bug: why does changing this value fail the test.
    private static final String MAP_ID_1 = "mockMapGraphId1";

    //TODO fs test bug: why does changing this value fail the test.
    private static final String ADDING_USER_ID = "user1";
    private static FederatedStoreCache federatedStoreCache;

    @AfterAll
    public static void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @BeforeEach
    public void setUp() {
        resetForFederatedTests();

        Properties properties = new Properties();
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, JcsCacheService.class.getName());
        properties.setProperty(CacheProperties.CACHE_CONFIG_FILE, "src/test/resources/gaffer-1.12.0-cache/cache.ccf");

        CacheServiceLoader.initialise(properties);
        federatedStoreCache = new FederatedStoreCache();

        new Graph.Builder().config(new GraphConfig(MAP_ID_1))
                .addStoreProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                .addSchema(loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON))
                .build();
    }

    @Test
    public void shouldReturnExpectedFederatedAccessUsingCacheDataFromVersion1_12() {
        final Set<String> graphAuths = new HashSet<>(asList("auth1", "auth2"));

        final FederatedAccess access = new FederatedAccess(graphAuths, ADDING_USER_ID);
        final FederatedAccess accessFromCacheVersion1_12 = federatedStoreCache.getAccessFromCache(MAP_ID_1);

        assertEquals(access.getReadAccessPredicate(), accessFromCacheVersion1_12.getReadAccessPredicate());
        assertEquals(access.getWriteAccessPredicate(), accessFromCacheVersion1_12.getWriteAccessPredicate());
        assertEquals(access.getOrDefaultReadAccessPredicate(), accessFromCacheVersion1_12.getOrDefaultReadAccessPredicate());
        assertEquals(access.getOrDefaultWriteAccessPredicate(), accessFromCacheVersion1_12.getOrDefaultWriteAccessPredicate());
    }
}
