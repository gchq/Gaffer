package uk.gov.gchq.gaffer.cache;


import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;

public class EmptyCacheService implements ICacheService {
    @Override
    public void initialise(Properties properties) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        return null;
    }

    @Override
    public <K, V> V getFromCache(String cacheName, K key) {
        return null;
    }

    @Override
    public <K, V> void putInCache(String cacheName, K key, V value) throws CacheOperationException {

    }

    @Override
    public <K, V> void putSafeInCache(String cacheName, K key, V value) throws CacheOperationException {

    }

    @Override
    public <K, V> void removeFromCache(String cacheName, K key) {

    }

    @Override
    public <K, V> Collection<V> getAllValuesFromCache(String cacheName) {
        return null;
    }

    @Override
    public <K, V> Set<K> getAllKeysFromCache(String cacheName) {
        return null;
    }

    @Override
    public int sizeOfCache(String cacheName) {
        return 0;
    }

    @Override
    public void clearCache(String cacheName) throws CacheOperationException {

    }
}
