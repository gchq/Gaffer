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
import uk.gov.gchq.gaffer.cache.ICacheService;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import java.util.Collections;
import java.util.Set;

public class Cache<V> {
    public static final String ERROR_ADDING_KEY_TO_CACHE_KEY_S = "Error adding key to cache. key: %s";
    protected String cacheServiceName;

    public Cache(final String cacheServiceName) {
        this.cacheServiceName = cacheServiceName;
    }

    public V getFromCache(final String key) {
        return CacheServiceLoader.getService().getFromCache(cacheServiceName, key);
    }

    public String getCacheServiceName() {
        return cacheServiceName;
    }

    protected void addToCache(final String key, final V value, final boolean overwrite) throws CacheOperationException {
        final ICacheService service = CacheServiceLoader.getService();
        try {
            if (overwrite) {
                service.putInCache(getCacheServiceName(), key, value);
            } else {
                service.putSafeInCache(getCacheServiceName(), key, value);
            }
        } catch (final CacheOperationException e) {
            throw new CacheOperationException(String.format(ERROR_ADDING_KEY_TO_CACHE_KEY_S, key), e);
        }
    }

    public Set<String> getAllKeys() {
        final Set<String> allKeysFromCache = CacheServiceLoader.getService().getAllKeysFromCache(cacheServiceName);
        return (null == allKeysFromCache) ? null : Collections.unmodifiableSet(allKeysFromCache);
    }

    /**
     * Clear the cache.
     *
     * @throws CacheOperationException if there was an error trying to clear the cache
     */
    public void clearCache() throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(cacheServiceName);
    }

    public boolean contains(final String graphId) {
        return getAllKeys().contains(graphId);
    }

    /**
     * Delete the {@link uk.gov.gchq.gaffer.graph.Graph} related to the specified ID from the cache.
     *
     * @param graphId the ID of the {@link uk.gov.gchq.gaffer.graph.Graph} to be deleted
     */
    public void deleteFromCache(final String graphId) {
        CacheServiceLoader.getService().removeFromCache(cacheServiceName, graphId);
    }

    /**
     * Get the cache for the FederatedStore.
     *
     * @return the FederatedStore cache
     */
    public ICache getCache() {
        if (CacheServiceLoader.getService() != null) {
            return CacheServiceLoader.getService().getCache(cacheServiceName);
        } else {
            return null;
        }
    }
}
