/*
 * Copyright 2017-2024 Crown Copyright
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
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

/**
 * Wrapper around {@link FederatedStoreCacheTransient} to provide an interface
 * for
 * handling {@link Graph}s within a {@link FederatedStore}.
 *
 * @deprecated Federated store will use the default cache implementation going
 *             forward.
 */
@Deprecated
public final class FederatedStoreCache {
    private final FederatedStoreCacheTransient cacheTransient;

    public FederatedStoreCache(final String suffixFederatedStoreCacheName) {
        cacheTransient = new FederatedStoreCacheTransient(suffixFederatedStoreCacheName);
    }

    /**
     * Get all the ID's related to the {@link Graph}'s stored in the cache.
     *
     * @return all the Graph ID's within the cache.
     */
    public Iterable<String> getAllGraphIds() {
        return cacheTransient.getAllGraphIds();
    }

    /**
     * Add the specified {@link Graph} to the cache.
     *
     * @param graph     the {@link Graph} to be added
     * @param overwrite if true, overwrite any graphs already in the cache with the
     *                  same ID
     * @param access    Access for the graph being stored.
     * @throws CacheOperationException if issues adding to cache
     */
    public void addGraphToCache(final Graph graph, final FederatedAccess access, final boolean overwrite)
            throws CacheOperationException {
        try {
            cacheTransient.addGraphToCache(graph, JSONSerialiser.serialise(access), overwrite);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add the specified {@link Graph} to the cache.
     *
     * @param graphSerialisable the serialised {@link Graph} to be added
     * @param access            Access for the graph being stored.
     * @param overwrite         if true, overwrite any graphs already in the cache
     *                          with the same ID
     * @throws CacheOperationException if issues adding to cache
     */
    @SuppressWarnings("PMD.PreserveStackTrace") //False positive
    public void addGraphToCache(final GraphSerialisable graphSerialisable, final FederatedAccess access, final boolean overwrite)
            throws CacheOperationException {
        try {
            cacheTransient.addGraphToCache(graphSerialisable, JSONSerialiser.serialise(access), overwrite);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteGraphFromCache(final String graphId) {
        cacheTransient.deleteGraphFromCache(graphId);
    }

    /**
     * Retrieve the {@link GraphSerialisable} with the specified ID from the cache.
     *
     * @param graphId the ID of the {@link Graph} to retrieve
     * @return the {@link GraphSerialisable} related to the specified ID
     * @throws CacheOperationException exception
     */
    public GraphSerialisable getGraphFromCache(final String graphId) throws CacheOperationException {
        return cacheTransient.getGraphFromCache(graphId);
    }

    /**
     * Retrieve the {@link Graph} with the specified ID from the cache.
     *
     * @param graphId the ID of the {@link Graph} to retrieve
     * @return the {@link Graph} related to the specified ID
     */
    public GraphSerialisable getGraphSerialisableFromCache(final String graphId) {
        try {
            return cacheTransient.getGraphSerialisableFromCache(graphId);
        } catch (final CacheOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public void addGraphToCache(final GraphSerialisable graphSerialisable, final byte[] access, final boolean overwrite) throws CacheOperationException {
        cacheTransient.addGraphToCache(graphSerialisable, access, overwrite);
    }

    public FederatedAccess getAccessFromCache(final String graphId) {
        try {
            final byte[] accessFromCache = cacheTransient.getAccessFromCache(graphId);
            return accessFromCache == null ? null : JSONSerialiser.deserialise(accessFromCache, FederatedAccess.class);
        } catch (final Exception e) {
            throw new GafferRuntimeException(String.format("Error Getting Access from Cache for graphId:%s", graphId), e);
        }
    }

    public Pair<GraphSerialisable, FederatedAccess> getFromCache(final String key) {
        try {
            final Pair<GraphSerialisable, byte[]> fromCache = cacheTransient.getFromCache(key);
            return new Pair<>(fromCache.getFirst(), JSONSerialiser.deserialise(fromCache.getSecond(), FederatedAccess.class));
        } catch (final Exception e) {
            throw new RuntimeException("Error deserialising FederatedAccess object from cache", e);
        }
    }

    public String getCacheName() {
        return cacheTransient.getCacheName();
    }

    public Iterable<String> getAllKeys() {
        return cacheTransient.getAllKeys();
    }

    public void clearCache() throws CacheOperationException {
        cacheTransient.clearCache();
    }

    public boolean contains(final String graphId) {
        return cacheTransient.contains(graphId);
    }

    public void deleteFromCache(final String key) {
        cacheTransient.deleteFromCache(key);
    }

    public ICache getCache() {
        return cacheTransient.getCache();
    }

    public static String getCacheNameFrom(final String suffixCacheName) {
        return FederatedStoreCacheTransient.getCacheNameFrom(suffixCacheName);
    }
}
