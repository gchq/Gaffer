package uk.gov.gchq.gaffer.cache.impl;


import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;

import java.util.HashMap;

public class HashMapCacheService implements ICacheService {

    HashMap<String, HashMapCache> caches;

    @Override
    public void initialise() {

    }

    @Override
    public void shutDown() {
        // do nothing
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
