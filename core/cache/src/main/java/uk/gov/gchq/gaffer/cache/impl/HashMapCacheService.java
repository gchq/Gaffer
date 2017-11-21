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
    public static final String STATIC_CACHE = "gaffer.cache.hashmap.static";
    public static final String JAVA_SERIALISATION_CACHE = "gaffer.cache.hashmap.useJavaSerialisation";
    private static final HashMap<String, HashMapCache> STATIC_CACHES = new HashMap<>();
    private final HashMap<String, HashMapCache> nonStaticCaches = new HashMap<>();
    private boolean useJavaSerialisation = false;

    private HashMap<String, HashMapCache> caches = nonStaticCaches;

    @Override
    public void initialise(final Properties properties) {
        if (properties != null) {
            useJavaSerialisation = Boolean.parseBoolean(properties.getProperty(JAVA_SERIALISATION_CACHE));
        }

        if (properties != null && Boolean.parseBoolean(properties.getProperty(STATIC_CACHE))) {
            caches = STATIC_CACHES;
        } else {
            caches = nonStaticCaches;
        }
    }

    @Override
    public void shutdown() {
        caches.clear();
    }

    @Override
    public <K, V> ICache<K, V> getCache(final String cacheName) {
        HashMapCache<K, V> cache = caches.computeIfAbsent(cacheName, k -> new HashMapCache<>(useJavaSerialisation));

        return cache;
    }
}
