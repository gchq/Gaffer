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
