package uk.gov.gchq.gaffer.cache;


public class MockCacheService implements ICacheService {
    @Override
    public void initialise() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        return null;
    }
}
