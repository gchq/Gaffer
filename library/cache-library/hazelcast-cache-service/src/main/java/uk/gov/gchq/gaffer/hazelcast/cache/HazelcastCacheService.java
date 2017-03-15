package uk.gov.gchq.gaffer.hazelcast.cache;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;
import static uk.gov.gchq.gaffer.cache.util.CacheSystemProperty.CACHE_CONFIG_FILE;

public class HazelcastCacheService implements ICacheService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastCacheService.class);
    private static final HazelcastInstance HAZELCAST = Hazelcast.newHazelcastInstance();

    @Override
    public void initialise() {
        String configFile = System.getProperty(CACHE_CONFIG_FILE);

        if (configFile == null) {
            LOGGER.warn("Config file not set using system property: " + CACHE_CONFIG_FILE
                    + ". Using default settings");
        } else {

        }

        LOGGER.info(HAZELCAST.getCluster().getClusterState().name()); // bootstraps hazelcast
    }

    @Override
    public void shutDown() {
        HAZELCAST.shutdown();
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        IMap<K, V> cache = HAZELCAST.getMap(cacheName);
        return new HazelcastCache<>(cache);
    }
}
