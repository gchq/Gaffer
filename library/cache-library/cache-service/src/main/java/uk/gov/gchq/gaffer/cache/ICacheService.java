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
import java.util.Set;

/**
 * The cache service interface which enables the cache service loader to instantiate any service no matter the
 * implementation. All services should be able to provide a cache and methods to interact with it.
 *
 */
public interface ICacheService {

    void initialise();

    void shutdown();

    <K, V> ICache<K, V> getCache(final String cacheName);

    <K, V> V getFromCache(final String cacheName, final K key);

    <K, V> void putInCache(final String cacheName, final K key, final V value) throws CacheOperationException;

    <K, V> void putSafeInCache(final String cacheName, final K key, final V value) throws CacheOperationException;

    <K, V> void removeFromCache(final String cacheName, final K key);

    <K, V> Collection<V> getAllValuesFromCache(final String cacheName);

    <K, V> Set<K> getAllKeysFromCache(final String cacheName);

    int sizeOfCache(final String cacheName);

    void clearCache(final String cacheName) throws CacheOperationException;
}
