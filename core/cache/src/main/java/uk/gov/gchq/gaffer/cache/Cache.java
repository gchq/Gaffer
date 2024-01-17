/*
 * Copyright 2018-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.cache.CacheServiceLoader.DEFAULT_SERVICE_NAME;

/**
 * Type safe cache, adding and getting is guaranteed to be same type.
 *
 * @param <V> The type of values to add and get.
 */
public class Cache<K, V> {
    public static final String ERROR_ADDING_KEY_TO_CACHE_KEY_S = "Error adding key to cache. key: %s";
    protected String cacheName;
    private final String serviceName;

    public Cache(final String cacheName, final String serviceName) {
        this.cacheName = cacheName;
        // Use the supplied cache service name if it exists, otherwise
        // fallback to the default cache service name
        if (CacheServiceLoader.isEnabled(serviceName)) {
            this.serviceName = serviceName;
        } else {
            this.serviceName = DEFAULT_SERVICE_NAME;
        }
    }

    public Cache(final String cacheName) {
        this.cacheName = cacheName;
        this.serviceName = DEFAULT_SERVICE_NAME;
    }

    public V getFromCache(final String key) throws CacheOperationException {
        return CacheServiceLoader.getService(serviceName).getFromCache(cacheName, key);
    }

    public String getCacheName() {
        return cacheName;
    }

    protected void addToCache(final K key, final V value, final boolean overwrite) throws CacheOperationException {
        try {
            final ICacheService service = CacheServiceLoader.getService(serviceName);
            if (overwrite) {
                service.putInCache(getCacheName(), key, value);
            } else {
                service.putSafeInCache(getCacheName(), key, value);
            }
        } catch (final CacheOperationException e) {
            throw new CacheOperationException(String.format(ERROR_ADDING_KEY_TO_CACHE_KEY_S, key), e);
        }
    }

    public Set<K> getAllKeys() {
        try {
            final Set<K> allKeysFromCache;
            if (CacheServiceLoader.isEnabled(serviceName)) {
                allKeysFromCache = CacheServiceLoader.getService(serviceName).getAllKeysFromCache(cacheName);
            } else {
                throw new GafferRuntimeException(String.format("Cache '%s' is not enabled, check it was initialised", serviceName));
            }
            return (null == allKeysFromCache) ? Collections.emptySet() : Collections.unmodifiableSet(allKeysFromCache);
        } catch (final Exception e) {
            throw new GafferRuntimeException("Error getting all keys", e);
        }
    }

    /**
     * Clear the cache.
     *
     * @throws CacheOperationException if there was an error trying to clear the cache
     */
    public void clearCache() throws CacheOperationException {
        CacheServiceLoader.getService(serviceName).clearCache(cacheName);
    }

    public boolean contains(final String graphId) {
        return getAllKeys().contains(graphId);
    }

    /**
     * Delete the value related to the specified ID from the cache.
     *
     * @param key the ID of the key to be deleted
     */
    public void deleteFromCache(final String key) {
        CacheServiceLoader.getService(serviceName).removeFromCache(cacheName, key);
    }

    /**
     * Get the cache.
     *
     * @return ICache
     */
    public ICache getCache() {
        if (CacheServiceLoader.isEnabled(serviceName)) {
            return CacheServiceLoader.getService(serviceName).getCache(cacheName);
        } else {
            return null;
        }
    }

    public String getSuffixCacheNameWithoutPrefix(final String prefixCacheServiceName) {
        return getCacheName().equals(prefixCacheServiceName)
                ? null
                : getCacheName().substring(prefixCacheServiceName.length() + 1);
    }

    public static String getCacheNameFrom(final String prefixCacheServiceName, final String suffixCacheName) {
        return String.format("%s%s", prefixCacheServiceName,
                nonNull(suffixCacheName)
                        ? "_" + suffixCacheName.toLowerCase(Locale.getDefault())
                        : "");
    }
}
