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
 * The cache service interface which enables the cache service loader to instantiate
 * any service no matter the implementation. All services should be able to provide
 * a cache and methods to interact with it.
 */
public interface ICacheService {

    /**
     * Initialise the cache service based on the supplied {@link Properties} object.
     *
     * @param properties the Properties object to apply to the cache service
     */
    void initialise(final Properties properties);

    /**
     * Shutdown the cache service.
     */
    void shutdown();

    /**
     * Get the named cache from the cache service.
     *
     * @param cacheName the name of the cache to retrieve
     * @param <K> The object type that acts as the key for the cache
     * @param <V> The value that is stored in the cache
     * @return the requested cache object
     */
    <K, V> ICache<K, V> getCache(final String cacheName);

    /**
     * Get the value associated with the specified cache and key.
     *
     * @param cacheName the name of the cache to look in
     * @param key the key of the entry to lookup
     * @param <K> The object type that acts as the key for the cache
     * @param <V> The value that is stored in the cache
     * @return the requested cache object
     */
    default <K, V> V getFromCache(final String cacheName, final K key) {
        final ICache<K, V> cache = getCache(cacheName);
        return cache.get(key);
    }

    /**
     * Add a new key-value pair to the specified cache.
     *
     * @param cacheName the name of the cache
     * @param key the key to add
     * @param value the value to add
     * @param <K> The object type that acts as the key for the cache
     * @param <V> The value that is stored in the cache
     * @throws CacheOperationException if there is an error adding the new key-value pair to the cache
     */
    default <K, V> void putInCache(final String cacheName, final K key, final V value) throws CacheOperationException {
        final ICache<K, V> cache = getCache(cacheName);
        cache.put(key, value);
    }

    /**
     * Add a new key-value pair to the specified cache, but only if there is existing
     * entry associated with the specified key.
     *
     * @param cacheName the name of the cache
     * @param key the key to add
     * @param value the value to add
     * @param <K> The object type that acts as the key for the cache
     * @param <V> The value that is stored in the cache
     * @throws CacheOperationException if the specified key already exists in the cache with a non-null value
     */
    default <K, V> void putSafeInCache(final String cacheName, final K key, final V value) throws CacheOperationException {
        final ICache<K, V> cache = getCache(cacheName);
        cache.putSafe(key, value);
    }

    /**
     * Remove the entry associated with the specified key from the specified cache.
     *
     * @param cacheName the name of the cache to look in
     * @param key the key of the entry to remove
     * @param <K> The object type that acts as the key for the cache
     * @param <V> The value that is stored in the cache
     */
    default <K, V> void removeFromCache(final String cacheName, final K key) {
        final ICache<K, V> cache = getCache(cacheName);
        cache.remove(key);
    }

    /**
     * Get all of the values associated with the specified cache.
     *
     * @param cacheName the name of the cache to look in
     * @param <K> The object type that acts as the key for the cache
     * @param <V> The value that is stored in the cache
     * @return the requested cache objects
     */
    default <K, V> Collection<V> getAllValuesFromCache(final String cacheName) {
        final ICache<K, V> cache = getCache(cacheName);
        return cache.getAllValues();
    }

    /**
     * Get all of the keys associated with the specified cache.
     *
     * @param cacheName the name of the cache to look in
     * @param <K> The object type that acts as the key for the cache
     * @param <V> The value that is stored in the cache
     * @return the requested cache keys
     */
    default <K, V> Set<K> getAllKeysFromCache(final String cacheName) {
        final ICache<K, V> cache = getCache(cacheName);
        return cache.getAllKeys();
    }

    /**
     * Retrieve the size of the specified cache.
     *
     * @param cacheName the name of the cache
     * @return the size of the cache
     */
    default int sizeOfCache(final String cacheName) {
        return getCache(cacheName).size();
    }

    /**
     * Clear the contents of the specified cache.
     *
     * @param cacheName the name of the cache to clear
     * @throws CacheOperationException if there was an error clearing the cache
     */
    default void clearCache(final String cacheName) throws CacheOperationException {
        getCache(cacheName).clear();
    }
}
