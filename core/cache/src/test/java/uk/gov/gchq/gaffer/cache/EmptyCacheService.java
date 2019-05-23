/*
 * Copyright 2016-2019 Crown Copyright
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

public class EmptyCacheService implements ICacheService {
    @Override
    public void initialise(final Properties properties) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public <K, V> ICache<K, V> getCache(final String cacheName) {
        return null;
    }

    @Override
    public <K, V> V getFromCache(final String cacheName, final K key) {
        return null;
    }

    @Override
    public <K, V> void putInCache(final String cacheName, final K key, final V value) throws CacheOperationException {

    }

    @Override
    public <K, V> void putSafeInCache(final String cacheName, final K key, final V value) throws CacheOperationException {

    }

    @Override
    public <K, V> void removeFromCache(final String cacheName, final K key) {

    }

    @Override
    public <K, V> Collection<V> getAllValuesFromCache(final String cacheName) {
        return null;
    }

    @Override
    public <K, V> Set<K> getAllKeysFromCache(final String cacheName) {
        return null;
    }

    @Override
    public int sizeOfCache(final String cacheName) {
        return 0;
    }

    @Override
    public void clearCache(final String cacheName) throws CacheOperationException {

    }
}
