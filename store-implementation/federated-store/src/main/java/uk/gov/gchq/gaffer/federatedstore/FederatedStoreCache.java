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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.Set;

import static java.util.Objects.isNull;

/**
 * Wrapper around the {@link uk.gov.gchq.gaffer.cache.CacheServiceLoader} to provide an interface for
 * handling the {@link Graph}s within a {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
public class FederatedStoreCache extends Cache<String, Pair<GraphSerialisable, FederatedAccess>> {

    private final FederatedStoreCacheTransient cacheTransient;

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
        try {
            cacheTransient.addGraphToCache(graph, JSONSerialiser.serialise(access), overwrite);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    public void addGraphToCache(final GraphSerialisable graphSerialisable, final FederatedAccess access, final boolean overwrite) throws CacheOperationException {
        try {
            cacheTransient.addGraphToCache(graphSerialisable, JSONSerialiser.serialise(access), overwrite);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteGraphFromCache(final String graphId) {
        cacheTransient.deleteGraphFromCache(graphId);
    }

    public GraphSerialisable getGraphFromCache(final String graphId) throws CacheOperationException {
        return cacheTransient.getGraphFromCache(graphId);
    }

    public GraphSerialisable getGraphSerialisableFromCache(final String graphId) {
        try {
            return cacheTransient.getGraphSerialisableFromCache(graphId);
        } catch (CacheOperationException e) {
            //TODO 404
            throw new RuntimeException(e);
        }
    }

    public void addGraphToCache(final GraphSerialisable graphSerialisable, final byte[] access, final boolean overwrite) throws CacheOperationException {
        cacheTransient.addGraphToCache(graphSerialisable, access, overwrite);
    }

    public FederatedAccess getAccessFromCache(final String graphId) {
        try {
        final byte[] accessFromCache = cacheTransient.getAccessFromCache(graphId);
            return (isNull(accessFromCache)) ? null : JSONSerialiser.deserialise(accessFromCache, FederatedAccess.class);
        } catch (final Exception e) {
            //TODO 404
            throw new RuntimeException(e);
        }
    }

    @Override
    public Pair<GraphSerialisable, FederatedAccess> getFromCache(final String key) {
        try {
        final Pair<GraphSerialisable, byte[]> fromCache = cacheTransient.getFromCache(key);
            return new Pair<>(fromCache.getFirst(), JSONSerialiser.deserialise(fromCache.getSecond(), FederatedAccess.class));
        } catch (final Exception e) {
            //todo 404
            throw new RuntimeException(e);
        }
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
