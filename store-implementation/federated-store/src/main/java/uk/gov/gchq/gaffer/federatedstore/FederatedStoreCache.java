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

import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;

import java.util.Set;

import static java.util.Objects.isNull;

/**
 * Wrapper around the {@link uk.gov.gchq.gaffer.cache.CacheServiceLoader} to provide an interface for
 * handling the {@link Graph}s within a {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
public class FederatedStoreCache extends Cache<Pair<GraphSerialisable, FederatedAccess>> {
    public static final String ERROR_ADDING_GRAPH_TO_CACHE_GRAPH_ID_S = "Error adding graph to cache. graphId: %s";

    public FederatedStoreCache() {
        super("federatedStoreGraphs");
    }

    /**
     * Get all the ID's related to the {@link Graph}'s stored in the cache.
     *
     * @return all the Graph ID's within the cache as unmodifiable set.
     */
    public Set<String> getAllGraphIds() {
        return super.getAllKeys();
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
        addGraphToCache(new GraphSerialisable.Builder().graph(graph).build(), access, overwrite);
    }

    /**
     * Add the specified {@link Graph} to the cache.
     *
     * @param graphSerialisable the serialised {@link Graph} to be added
     * @param access            Access for the graph being stored.
     * @param overwrite         if true, overwrite any graphs already in the cache with the same ID
     * @throws CacheOperationException if there was an error trying to add to the cache
     */
    public void addGraphToCache(final GraphSerialisable graphSerialisable, final FederatedAccess access, final boolean overwrite) throws CacheOperationException {
        String graphId = graphSerialisable.getGraphId();
        Pair<GraphSerialisable, FederatedAccess> pair = new Pair<>(graphSerialisable, access);
        try {
            addToCache(graphId, pair, overwrite);
        } catch (final CacheOperationException e) {
            throw new CacheOperationException(String.format(ERROR_ADDING_GRAPH_TO_CACHE_GRAPH_ID_S, graphId), e.getCause());
        }
    }

    public void deleteGraphFromCache(final String graphId) {
        super.deleteFromCache(graphId);
    }

    /**
     * Retrieve the {@link Graph} with the specified ID from the cache.
     *
     * @param graphId the ID of the {@link Graph} to retrieve
     * @return the {@link Graph} related to the specified ID
     */
    public Graph getGraphFromCache(final String graphId) {
        final GraphSerialisable graphSerialisable = getGraphSerialisableFromCache(graphId);
        return (isNull(graphSerialisable)) ? null : graphSerialisable.getGraph();
    }

    /**
     * Retrieve the {@link Graph} with the specified ID from the cache.
     *
     * @param graphId the ID of the {@link Graph} to retrieve
     * @return the {@link Graph} related to the specified ID
     */
    public GraphSerialisable getGraphSerialisableFromCache(final String graphId) {
        final Pair<GraphSerialisable, FederatedAccess> fromCache = getFromCache(graphId);
        return (isNull(fromCache)) ? null : fromCache.getFirst();
    }

    public FederatedAccess getAccessFromCache(final String graphId) {
        final Pair<GraphSerialisable, FederatedAccess> fromCache = getFromCache(graphId);
        return (isNull(fromCache)) ? null : fromCache.getSecond();
    }
}
