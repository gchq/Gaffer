/*
 * Copyright 2016-2017 Crown Copyright
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
import java.util.Properties;
import java.util.Set;

/**
 * The cache service interface which enables the cache service loader to instantiate any service no matter the
 * implementation. All services should be able to provide a cache and methods to interact with it.
 */
public interface ICacheService {

    void initialise(final Properties properties);

    void shutdown();

    <K, V> ICache<K, V> getCache(final String cacheName);

    default <K, V> V getFromCache(final String cacheName, final K key) {
        ICache<K, V> cache = getCache(cacheName);
        return cache.get(key);
    }

    default <K, V> void putInCache(final String cacheName, final K key, final V value) throws CacheOperationException {
        ICache<K, V> cache = getCache(cacheName);
        cache.put(key, value);
    }

    default <K, V> void putSafeInCache(final String cacheName, final K key, final V value) throws CacheOperationException {
        ICache<K, V> cache = getCache(cacheName);
        cache.putSafe(key, value);
    }

    default <K, V> void removeFromCache(final String cacheName, final K key) {
        ICache<K, V> cache = getCache(cacheName);
        cache.remove(key);
    }

    default <K, V> Collection<V> getAllValuesFromCache(final String cacheName) {
        ICache<K, V> cache = getCache(cacheName);
        return cache.getAllValues();
    }

    default <K, V> Set<K> getAllKeysFromCache(final String cacheName) {
        ICache<K, V> cache = getCache(cacheName);
        return cache.getAllKeys();
    }

    default int sizeOfCache(final String cacheName) {
        return getCache(cacheName).size();
    }

    default void clearCache(final String cacheName) throws CacheOperationException {
        getCache(cacheName).clear();
    }
}
