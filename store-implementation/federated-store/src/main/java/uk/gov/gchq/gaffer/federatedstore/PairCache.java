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
public class PairCache<K, V1, V2> {
    private final String cacheServiceName;

    public PairCache(final String cacheServiceName) {
        this.cacheServiceName = cacheServiceName;
    }

    public void putSafeInCache(final Graph graph, final Pair<V1, V2> pair) throws CacheOperationException {
        CacheServiceLoader.getService().putSafeInCache(this.cacheServiceName, graph.getGraphId(), pair);
    }

    public void putInCache(final Graph graph, final Pair<V1, V2> pair) throws CacheOperationException {
        CacheServiceLoader.getService().putInCache(this.cacheServiceName, graph.getGraphId(), pair);
    }

    public boolean isServiceNull() {
        return CacheServiceLoader.getService() != null;
    }

    public Pair<V1, V2> getFromCache(final K key) {
        return CacheServiceLoader.getService().getFromCache(cacheServiceName, key);
    }

    public void initialise(final Properties properties) {
        CacheServiceLoader.getService().initialise(properties);
    }

    public Set<K> getAllKeysFromCache() {
        return CacheServiceLoader.getService().getAllKeysFromCache(cacheServiceName);
    }

    public void putInCache(final K key, final Pair<V1, V2> value) throws CacheOperationException {
        CacheServiceLoader.getService().putInCache(cacheServiceName, key, value);
    }

    public ICache<K, Pair<V1, V2>> getCache() {
        return CacheServiceLoader.getService().getCache(cacheServiceName);
    }

    public void putSafeInCache(final K key, final Pair<V1, V2> value) throws CacheOperationException {
        CacheServiceLoader.getService().putSafeInCache(cacheServiceName, key, value);
    }

    public void removeFromCache(final K key) {
        CacheServiceLoader.getService().removeFromCache(cacheServiceName, key);
    }

    public Collection<Pair<V1, V2>> getAllValuesFromCache() {
        return CacheServiceLoader.getService().getAllValuesFromCache(cacheServiceName);
    }

    public void clearCache() throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(cacheServiceName);
    }

    public int sizeOfCache() {
        return CacheServiceLoader.getService().sizeOfCache(cacheServiceName);
    }

    public void shutdown() {
        CacheServiceLoader.getService().shutdown();
    }

}
