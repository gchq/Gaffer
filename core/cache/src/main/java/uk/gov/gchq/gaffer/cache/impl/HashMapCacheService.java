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

package uk.gov.gchq.gaffer.cache.impl;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;

import java.util.HashMap;
import java.util.Properties;

/**
 * Simple implementation of the {@link ICacheService} interface which uses a
 * {@link HashMapCache} as the cache implementation.
 */
public class HashMapCacheService implements ICacheService {

    private final HashMap<String, HashMapCache> caches = new HashMap<>();

    @Override
    public void initialise(final Properties properties) {
        // do nothing
    }

    @Override
    public void shutdown() {
        caches.clear();
    }

    @Override
    public <K, V> ICache<K, V> getCache(final String cacheName) {
        HashMapCache<K, V> cache = caches.computeIfAbsent(cacheName, k -> new HashMapCache<>());

        return cache;
    }
}
