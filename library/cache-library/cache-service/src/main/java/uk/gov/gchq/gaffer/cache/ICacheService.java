package uk.gov.gchq.gaffer.cache;


public interface ICacheService {

    void initialise();

    void shutDown();

    <K, V> ICache<K, V> getCache(final String cacheName);
}
