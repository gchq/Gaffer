/*
 * Copyright 2017-2020 Crown Copyright
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;

import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FederatedStoreCacheTest {
    private static final String PATH_MAP_STORE_PROPERTIES = "properties/singleUseMiniAccStore.properties";
    private static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String MAP_ID_1 = "mockMapGraphId1";
    private static Graph testGraph;
    private static FederatedStoreCache federatedStoreCache;
    private static Properties properties = new Properties();

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, PATH_MAP_STORE_PROPERTIES));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @BeforeAll
    public static void setUp(@TempDir Path tempDir) {
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);
        CacheServiceLoader.initialise(properties);
        federatedStoreCache = new FederatedStoreCache();

        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());

        testGraph = new Graph.Builder().config(new GraphConfig(MAP_ID_1))
                .addStoreProperties(PROPERTIES)
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @BeforeEach
    public void beforeEach() throws CacheOperationException {
        federatedStoreCache.clearCache();
    }

    @Test
    public void shouldAddAndGetGraphToCache() throws CacheOperationException {
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        Graph cached = federatedStoreCache.getGraphFromCache(MAP_ID_1);

        assertEquals(testGraph.getGraphId(), cached.getGraphId());
        assertEquals(testGraph.getSchema().toString(), cached.getSchema().toString());
        assertEquals(testGraph.getStoreProperties(), cached.getStoreProperties());
    }

    @Test
    public void shouldGetAllGraphIdsFromCache() throws CacheOperationException {
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        Set<String> cachedGraphIds = federatedStoreCache.getAllGraphIds();
        assertEquals(1, cachedGraphIds.size());
        assertTrue(cachedGraphIds.contains(testGraph.getGraphId()));
    }

    @Test
    public void shouldDeleteFromCache() throws CacheOperationException {
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        Set<String> cachedGraphIds = federatedStoreCache.getAllGraphIds();
        assertEquals(1, cachedGraphIds.size());
        assertTrue(cachedGraphIds.contains(testGraph.getGraphId()));

        federatedStoreCache.deleteFromCache(testGraph.getGraphId());
        Set<String> cachedGraphIdsAfterDelete = federatedStoreCache.getAllGraphIds();
        assertEquals(0, cachedGraphIdsAfterDelete.size());
    }

    @Test
    public void shouldThrowExceptionIfGraphAlreadyExistsInCache() throws CacheOperationException {
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        try {
            federatedStoreCache.addGraphToCache(testGraph, null, false);
            fail("Exception expected");
        } catch (OverwritingException e) {
            assertTrue(e.getMessage().contains("Cache entry already exists"));
        }
    }

    @Test
    public void shouldThrowExceptionIfGraphIdToBeRemovedIsNull() throws CacheOperationException {
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        federatedStoreCache.deleteFromCache(null);
        assertEquals(1, federatedStoreCache.getAllGraphIds().size());
    }

    @Test
    public void shouldThrowExceptionIfGraphIdToGetIsNull() throws CacheOperationException {
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        assertNull(federatedStoreCache.getGraphFromCache(null));
    }
}
