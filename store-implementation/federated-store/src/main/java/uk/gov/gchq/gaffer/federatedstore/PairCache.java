/*
 * Copyright 2018 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;

/**
 * Wrapper around the {@link CacheServiceLoader} to provide an interface for
 * handling cache where the value is a {@link Pair}.
 *
 * @param <K>  key
 * @param <V1> first item of value pair
 * @param <V2> second item of value pair
 */
public abstract class PairCache<K, V1, V2> {
    protected final String cacheServiceName = getCacheServiceName();

    protected abstract String getCacheServiceName();

    protected void putSafeInCache(final Graph graph, final Pair<V1, V2> pair) throws CacheOperationException {
        CacheServiceLoader.getService().putSafeInCache(this.cacheServiceName, graph.getGraphId(), pair);
    }

    protected void putInCache(final Graph graph, final Pair<V1, V2> pair) throws CacheOperationException {
        CacheServiceLoader.getService().putInCache(this.cacheServiceName, graph.getGraphId(), pair);
    }

    protected boolean isServiceNull() {
        return CacheServiceLoader.getService() != null;
    }

    protected Pair<V1, V2> getFromCache(final String cacheName, final K key) {
        return CacheServiceLoader.getService().getFromCache(cacheName, key);
    }

    protected void initialise(final Properties properties) {
        CacheServiceLoader.getService().initialise(properties);
    }

    protected Set<K> getAllKeysFromCache(final String cacheName) {
        return CacheServiceLoader.getService().getAllKeysFromCache(cacheName);
    }

    protected void putInCache(final String cacheName, final K key, final Pair<V1, V2> value) throws CacheOperationException {
        CacheServiceLoader.getService().putInCache(cacheName, key, value);
    }

    protected ICache<K, Pair<V1, V2>> getCache(final String cacheName) {
        return CacheServiceLoader.getService().getCache(cacheName);
    }

    protected void putSafeInCache(final String cacheName, final K key, final Pair<V1, V2> value) throws CacheOperationException {
        CacheServiceLoader.getService().putSafeInCache(cacheName, key, value);
    }

    protected void removeFromCache(final String cacheName, final K key) {
        CacheServiceLoader.getService().removeFromCache(cacheName, key);
    }

    protected Collection<Pair<V1, V2>> getAllValuesFromCache(final String cacheName) {
        return CacheServiceLoader.getService().getAllValuesFromCache(cacheName);
    }

    protected void clearCache(final String cacheName) throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(cacheName);
    }

    protected int sizeOfCache(final String cacheName) {
        return CacheServiceLoader.getService().sizeOfCache(cacheName);
    }

    protected void shutdown() {
        CacheServiceLoader.getService().shutdown();
    }

}
