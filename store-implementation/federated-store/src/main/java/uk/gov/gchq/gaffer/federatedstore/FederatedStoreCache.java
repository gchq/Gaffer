/*
 * Copyright 2017-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;

import java.util.Set;

import static java.util.Objects.isNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedAccess.Transient.getFederatedAccess;
import static uk.gov.gchq.gaffer.federatedstore.FederatedAccess.Transient.getTransient;

/**
 * Wrapper around the {@link uk.gov.gchq.gaffer.cache.CacheServiceLoader} to provide an interface for
 * handling the {@link Graph}s within a {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
public class FederatedStoreCache extends Cache<Pair<GraphSerialisable, FederatedAccess>> {

    final FederatedStoreCacheTransient cacheTransient;

    public FederatedStoreCache() {
        this(null);
    }

    public FederatedStoreCache(final String cacheName) {
        super(null);
        cacheTransient = new FederatedStoreCacheTransient(cacheName);
    }

    public Set<String> getAllGraphIds() {
        return cacheTransient.getAllGraphIds();
    }

    public void addGraphToCache(final Graph graph, final FederatedAccess access, final boolean overwrite) throws CacheOperationException {
        cacheTransient.addGraphToCache(graph, getTransient(access) , overwrite);
    }

    public void addGraphToCache(final GraphSerialisable graphSerialisable, final FederatedAccess access, final boolean overwrite) throws CacheOperationException {
        cacheTransient.addGraphToCache(graphSerialisable, getTransient(access), overwrite);
    }

    public void deleteGraphFromCache(final String graphId) {
        cacheTransient.deleteGraphFromCache(graphId);
    }

    public GraphSerialisable getGraphFromCache(final String graphId) {
        return cacheTransient.getGraphFromCache(graphId);
    }

    public GraphSerialisable getGraphSerialisableFromCache(final String graphId) {
        return cacheTransient.getGraphSerialisableFromCache(graphId);
    }

    public FederatedAccess getAccessFromCache(final String graphId) {
        final FederatedAccess.Transient accessFromCache = cacheTransient.getAccessFromCache(graphId);
        return (isNull(accessFromCache)) ? null : getFederatedAccess(accessFromCache);
    }

    @Override
    public Pair<GraphSerialisable, FederatedAccess> getFromCache(final String key) {
        final Pair<GraphSerialisable, FederatedAccess.Transient> fromCache = cacheTransient.getFromCache(key);
        return new Pair<>(fromCache.getFirst(), getFederatedAccess(fromCache.getSecond()));
    }

    @Override
    public String getCacheName() {
        return cacheTransient.getCacheName();
    }

    @Override
    public Set<String> getAllKeys() {
        return cacheTransient.getAllKeys();
    }

    @Override
    public void clearCache() throws CacheOperationException {
        cacheTransient.clearCache();
    }

    @Override
    public boolean contains(final String graphId) {
        return cacheTransient.contains(graphId);
    }

    @Override
    public void deleteFromCache(final String key) {
        cacheTransient.deleteFromCache(key);
    }

    @Override
    public ICache getCache() {
        return cacheTransient.getCache();
    }
}
