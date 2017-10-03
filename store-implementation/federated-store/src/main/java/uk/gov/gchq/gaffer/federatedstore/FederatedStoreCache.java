/*
 * Copyright 2017 Crown Copyright
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

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.graph.Graph;

import java.util.Set;

/**
 * Wrapper around the {@link CacheServiceLoader} to provide an
 * interface for handling the {@link uk.gov.gchq.gaffer.graph.Graph}s
 * within a {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
public class FederatedStoreCache {

    private static final String CACHE_SERVICE_NAME = "federatedStoreGraphs";

    public ICache getCache() {
        if (CacheServiceLoader.getService() != null) {
            return CacheServiceLoader.getService().getCache(CACHE_SERVICE_NAME);
        } else {
            return null;
        }
    }

    public Set<String> getAllGraphIds() {
        return CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME);
    }

    public void addToCache(Graph graph) throws CacheOperationException {
        CacheServiceLoader.getService().putInCache(CACHE_SERVICE_NAME, graph.getGraphId(), graph);
    }

    public void addSafeToCache(Graph graph) throws CacheOperationException {
        CacheServiceLoader.getService().putSafeInCache(CACHE_SERVICE_NAME, graph.getGraphId(), graph);
    }

    public Graph getFromCache(String graphId) throws CacheOperationException {
        final Graph graph = CacheServiceLoader.getService().getFromCache(CACHE_SERVICE_NAME, graphId);

        if (null != graph) {
            return graph;
        } else {
            throw new CacheOperationException("No graph found in the cache with graphId: " + graphId);
        }
    }

    public void deleteFromCache(String graphId) {
        CacheServiceLoader.getService().removeFromCache(CACHE_SERVICE_NAME, graphId);
    }

    public void clearCache() throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(CACHE_SERVICE_NAME);
    }
}
