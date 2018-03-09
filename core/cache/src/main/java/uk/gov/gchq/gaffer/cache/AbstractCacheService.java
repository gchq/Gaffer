/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.cache;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import java.util.Collection;
import java.util.Set;

/**
 * An abstract service which handles all the cache interaction methods. This leaves
 * only the {@code getCache()} method which will be fulfilled according to the cache
 * implementation.
 */
public abstract class AbstractCacheService implements ICacheService {

    @Override
    public abstract <K, V> ICache<K, V> getCache(final String cacheName);

    @Override
    public <K, V> V getFromCache(final String cacheName, final K key) {
        ICache<K, V> cache = getCache(cacheName);
        return cache.get(key);
    }

    @Override
    public <K, V> void putInCache(final String cacheName, final K key, final V value) throws CacheOperationException {
        ICache<K, V> cache = getCache(cacheName);
        cache.put(key, value);
    }

    @Override
    public <K, V> void putSafeInCache(final String cacheName, final K key, final V value) throws CacheOperationException {
        ICache<K, V> cache = getCache(cacheName);
        cache.putSafe(key, value);
    }

    @Override
    public <K, V> void removeFromCache(final String cacheName, final K key) {
        ICache<K, V> cache = getCache(cacheName);
        cache.remove(key);
    }

    @Override
    public <K, V> Collection<V> getAllValuesFromCache(final String cacheName) {
        ICache<K, V> cache = getCache(cacheName);
        return cache.getAllValues();
    }

    @Override
    public <K, V> Set<K> getAllKeysFromCache(final String cacheName) {
        ICache<K, V> cache = getCache(cacheName);
        return cache.getAllKeys();
    }

    @Override
    public int sizeOfCache(final String cacheName) {
        return getCache(cacheName).size();
    }

    @Override
    public void clearCache(final String cacheName) throws CacheOperationException {
        getCache(cacheName).clear();
    }
}
