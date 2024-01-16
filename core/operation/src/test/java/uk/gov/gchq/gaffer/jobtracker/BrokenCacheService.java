package uk.gov.gchq.gaffer.jobtracker;

import java.util.Properties;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;

public class BrokenCacheService implements ICacheService {

    @Override
    public void initialise(Properties properties) {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'initialise'");
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'shutdown'");
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        // TODO Auto-generated method stub
        return new BrokenICache<K, V>();
    }

}
