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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;

import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStoreCacheTest {
    private static Graph testGraph;
    private static FederatedStoreCache federatedStoreCache;

    @BeforeAll
    public static void setUp() {
        resetForFederatedTests();

        Properties properties = new Properties();
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);
        CacheServiceLoader.initialise(properties);

        federatedStoreCache = new FederatedStoreCache();
        testGraph = new Graph.Builder().config(new GraphConfig(GRAPH_ID_ACCUMULO))
                .addStoreProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                .addSchema(loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON))
                .build();
    }

    @BeforeEach
    public void beforeEach() throws CacheOperationException {
        federatedStoreCache.clearCache();
    }

    @Test
    public void shouldAddAndGetGraphToCache() throws CacheOperationException {
        //given
        federatedStoreCache.addGraphToCache(testGraph, null, false);

        //when
        Graph cached = federatedStoreCache.getGraphFromCache(GRAPH_ID_ACCUMULO);

        //then
        assertThat(cached)
                .isNotNull()
                .returns(testGraph.getGraphId(), from(Graph::getGraphId))
                .returns(testGraph.getSchema(), from(Graph::getSchema))
                .returns(testGraph.getStoreProperties(), from(Graph::getStoreProperties));
    }

    @Test
    public void shouldGetAllGraphIdsFromCache() throws CacheOperationException {
        //given
        federatedStoreCache.addGraphToCache(testGraph, null, false);

        //when
        Set<String> cachedGraphIds = federatedStoreCache.getAllGraphIds();

        //then
        assertThat(cachedGraphIds)
                .containsExactly(testGraph.getGraphId());
    }

    @Test
    public void shouldDeleteFromCache() throws CacheOperationException {
        //given
        shouldGetAllGraphIdsFromCache();

        //when
        federatedStoreCache.deleteGraphFromCache(testGraph.getGraphId());

        //then
        Set<String> cachedGraphIdsAfterDelete = federatedStoreCache.getAllGraphIds();
        assertThat(cachedGraphIdsAfterDelete).isEmpty();
    }

    @Test
    public void shouldThrowExceptionIfGraphAlreadyExistsInCache() throws CacheOperationException {
        //given
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        //when
        final OverwritingException exception = assertThrows(OverwritingException.class, () -> federatedStoreCache.addGraphToCache(testGraph, null, false));
        //then
        assertThat(exception).message().contains("Cache entry already exists");
    }

    @Test
    public void shouldNotThrowExceptionIfGraphIdToBeRemovedIsNull() throws CacheOperationException {
        //given
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        //when then
        assertDoesNotThrow(() -> federatedStoreCache.deleteGraphFromCache(null));
    }

    @Test
    public void shouldNotThrowExceptionIfGraphIdToGetIsNull() throws CacheOperationException {
        //given
        federatedStoreCache.addGraphToCache(testGraph, null, false);
        //when
        final Graph graphFromCache = assertDoesNotThrow(() -> federatedStoreCache.getGraphFromCache(null));
        //then
        assertThat(graphFromCache).isNull();
    }
}
