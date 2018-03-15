/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;

import java.util.Collections;
import java.util.Set;

/**
 * Wrapper around the {@link PairCache} to provide an interface for
 * handling the {@link Graph}s within a {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
public class FederatedStoreCache extends PairCache<String, GraphSerialisable, FederatedAccess> {

    public static final String ERROR_ADDING_GRAPH_TO_CACHE_GRAPH_ID_S = "Error adding graph to cache. graphId: %s";

    private Pair<GraphSerialisable, FederatedAccess> getFromCache(final String graphId) {
        return getFromCache(cacheServiceName, graphId);
    }

    /**
     * Get the cache for the FederatedStore.
     *
     * @return the FederatedStore cache
     */
    public ICache getCache() {
        if (isServiceNull()) {
            return getCache(cacheServiceName);
        } else {
            return null;
        }
    }

    /**
     * Get all the ID's related to the {@link Graph}'s stored in the cache.
     *
     * @return all the Graph ID's within the cache as unmodifiable set.
     */
    public Set<String> getAllGraphIds() {
        final Set<String> allKeysFromCache = getAllKeysFromCache(cacheServiceName);
        return (null == allKeysFromCache) ? null : Collections.unmodifiableSet(allKeysFromCache);
    }

    /**
     * Add the specified {@link Graph} to the cache.
     *
     * @param graph     the {@link Graph} to be added
     * @param overwrite if true, overwrite any graphs already in the cache with the same ID
     * @param access    Access for the graph being stored.
     * @throws CacheOperationException if there was an error trying to add to the cache
     */
    public void addGraphToCache(final Graph graph, final FederatedAccess access, final boolean overwrite) throws CacheOperationException {
        Pair<GraphSerialisable, FederatedAccess> pair = new Pair<>(new GraphSerialisable.Builder().graph(graph).build(), access);
        try {
            if (overwrite) {
                putInCache(graph, pair);
            } else {
                putSafeInCache(graph, pair);
            }
        } catch (final CacheOperationException e) {
            throw new CacheOperationException(String.format(ERROR_ADDING_GRAPH_TO_CACHE_GRAPH_ID_S, graph.getGraphId()), e);
        }
    }

    /**
     * Retrieve the {@link Graph} with the specified ID from the cache.
     *
     * @param graphId the ID of the {@link Graph} to retrieve
     * @return the {@link Graph} related to the specified ID
     */
    public Graph getGraphFromCache(final String graphId) {
        final Pair<GraphSerialisable, FederatedAccess> fromCache = getFromCache(graphId);
        final GraphSerialisable graphSerialisable = (null == fromCache) ? null : fromCache.getFirst();
        return (null == graphSerialisable) ? null : graphSerialisable.getGraph();
    }

    /**
     * Retrieve the {@link Graph} with the specified ID from the cache.
     *
     * @param graphId the ID of the {@link Graph} to retrieve
     * @return the {@link Graph} related to the specified ID
     */

    public GraphSerialisable getGraphSerialisableFromCache(final String graphId) {
        final Pair<GraphSerialisable, FederatedAccess> fromCache = getFromCache(graphId);
        return (null == fromCache) ? null : fromCache.getFirst();
    }

    public FederatedAccess getAccessFromCache(final String graphId) {
        final Pair<GraphSerialisable, FederatedAccess> fromCache = getFromCache(graphId);
        return fromCache.getSecond();
    }

    /**
     * Delete the {@link Graph} related to the specified ID from the cache.
     *
     * @param graphId the ID of the {@link Graph} to be deleted
     */
    public void deleteFromCache(final String graphId) {
        removeFromCache(cacheServiceName, graphId);
    }

    /**
     * Clear the cache.
     *
     * @throws CacheOperationException if there was an error trying to clear the cache
     */
    public void clearCache() throws CacheOperationException {
        clearCache(cacheServiceName);
    }

    public boolean contains(final String graphId) {
        return getAllGraphIds().contains(graphId);
    }

    @Override
    protected String getCacheServiceName() {
        return  "federatedStoreGraphs";
    }
}
