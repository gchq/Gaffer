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
import uk.gov.gchq.gaffer.graph.GraphSerialisable;

import java.util.Set;

/**
 * Wrapper around the {@link CacheServiceLoader} to provide an
 * interface for handling the {@link uk.gov.gchq.gaffer.graph.Graph}s
 * within a {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
public class FederatedStoreCache {

    private static final String CACHE_SERVICE_NAME = "federatedStoreGraphs";

    /**
     * Get the cache for the FederatedStore.
     *
     * @return the FederatedStore cache
     */
    public ICache getCache() {
        if (CacheServiceLoader.getService() != null) {
            return CacheServiceLoader.getService().getCache(CACHE_SERVICE_NAME);
        } else {
            return null;
        }
    }

    /**
     * Get all the ID's related to the {@link uk.gov.gchq.gaffer.graph.Graph}'s
     * stored in the cache.
     *
     * @return all the Graph ID's within the cache
     */
    public Set<String> getAllGraphIds() {
        return CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME);
    }

    /**
     * Add the specified {@link uk.gov.gchq.gaffer.graph.Graph} to the cache.
     *
     * @param graph     the {@link uk.gov.gchq.gaffer.graph.Graph} to be added
     * @param overwrite if true, overwrite any graphs already in the cache with the same ID
     * @throws CacheOperationException if there was an error trying to add to the cache
     */
    public void addGraphToCache(final Graph graph, final boolean overwrite) throws CacheOperationException {
        GraphSerialisable graphSerialisable = new GraphSerialisable.Builder().graph(graph).build();

        if (overwrite) {
            CacheServiceLoader.getService().putInCache(CACHE_SERVICE_NAME, graph.getGraphId(), graphSerialisable);
        } else {
            CacheServiceLoader.getService().putSafeInCache(CACHE_SERVICE_NAME, graph.getGraphId(), graphSerialisable);
        }
    }

    /**
     * Retrieve the {@link uk.gov.gchq.gaffer.graph.Graph} with the specified ID from the cache.
     *
     * @param graphId the ID of the {@link uk.gov.gchq.gaffer.graph.Graph} to retrieve
     * @return the {@link uk.gov.gchq.gaffer.graph.Graph} related to the specified ID
     * @throws CacheOperationException if there was an error trying to access the cache
     */
    public Graph getFromCache(final String graphId) throws CacheOperationException {
        if (null == graphId) {
            throw new CacheOperationException("Graph ID cannot be null");
        }

        final GraphSerialisable graphSerialisable = CacheServiceLoader.getService().getFromCache(CACHE_SERVICE_NAME, graphId);

        if (null != graphSerialisable) {
            return graphSerialisable.buildGraph();
        }
        throw new CacheOperationException("No graph in the cache with Graph ID: " + graphId);
    }

    /**
     * Delete the {@link uk.gov.gchq.gaffer.graph.Graph} related to the specified ID from the cache.
     *
     * @param graphId the ID of the {@link uk.gov.gchq.gaffer.graph.Graph} to be deleted
     * @throws CacheOperationException if there was an error trying to delete from the cache
     */
    public void deleteFromCache(final String graphId) throws CacheOperationException {
        if (null == graphId) {
            throw new CacheOperationException("Graph ID cannot be null");
        }

        CacheServiceLoader.getService().removeFromCache(CACHE_SERVICE_NAME, graphId);

        if (null != CacheServiceLoader.getService().getFromCache(CACHE_SERVICE_NAME, graphId)) {
            throw new CacheOperationException("Failed to remove Graph with ID: " + graphId + " from cache");
        }
    }

    /**
     * Clear the cache.
     *
     * @throws CacheOperationException if there was an error trying to clear the cache
     */
    public void clearCache() throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(CACHE_SERVICE_NAME);
    }
}
