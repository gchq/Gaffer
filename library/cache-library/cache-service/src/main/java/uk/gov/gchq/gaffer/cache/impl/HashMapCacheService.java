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

import java.util.HashMap;

public class HashMapCacheService extends AbstractCacheService {

    private HashMap<String, HashMapCache> caches = new HashMap<>();

    @Override
    public void initialise() {
        // do nothing
    }

    @Override
    public void shutdown() {
        caches.clear();
    }

    @Override
    public <K, V> ICache<K, V> getCache(final String cacheName) {
        HashMapCache cache = caches.get(cacheName);

        if (cache == null) {
            cache = new HashMapCache<K, V>();
            caches.put(cacheName, cache);
        }

        return cache;
    }
}
