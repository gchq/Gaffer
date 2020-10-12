/*
 * Copyright 2020 Crown Copyright
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.user.User;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FederatedStoreCacheBackwardCompatibilityTest {

    private static final String PATH_MAP_STORE_PROPERTIES = "properties/singleUseMiniAccStore.properties";
    private static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.JcsCacheService";
    private static final String MAP_ID_1 = "mockMapGraphId1";
    private static FederatedStoreCache federatedStoreCache;
    private static Properties properties = new Properties();

    private static Class currentClass = new Object() {}.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, PATH_MAP_STORE_PROPERTIES));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @BeforeAll
    public static void setUp(@TempDir Path tempDir) {
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);
        properties.setProperty(CacheProperties.CACHE_CONFIG_FILE, "src/test/resources/gaffer-1.12.0-cache/cache.ccf");

        CacheServiceLoader.initialise(properties);
        federatedStoreCache = new FederatedStoreCache();

        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());

        new Graph.Builder().config(new GraphConfig(MAP_ID_1))
                .addStoreProperties(PROPERTIES)
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @Test
    public void shouldReturnExpectedFederatedAccessUsingCacheDataFromVersion1_12() {
        final User addingUser = new User("user1");
        final Set<String> graphAuths = new HashSet<>(asList("auth1", "auth2"));

        final FederatedAccess access = new FederatedAccess(graphAuths, addingUser.getUserId());
        final FederatedAccess accessFromCacheVersion1_12 = federatedStoreCache.getAccessFromCache(MAP_ID_1);

        assertEquals(access.getReadAccessPredicate(), accessFromCacheVersion1_12.getReadAccessPredicate());
        assertEquals(access.getWriteAccessPredicate(), accessFromCacheVersion1_12.getWriteAccessPredicate());
    }
}
