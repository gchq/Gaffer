package uk.gov.gchq.gaffer.hazelcast.cache;

import com.hazelcast.core.IMap;
import uk.gov.gchq.gaffer.cache.ICache;

import java.util.Collection;
import java.util.Set;

public class HazelcastCache <K, V> implements ICache <K, V> {
    private IMap<K, V> distributedMap;

    public HazelcastCache(IMap <K, V> distributedMap) {
        this.distributedMap = distributedMap;
    }

    @Override
    public V get(K key) {
        return distributedMap.get(key);
    }

    @Override
    public void put(K key, V value) {
        distributedMap.put(key, value);
    }

    @Override
    public Collection<V> getAllValues() {
        return distributedMap.values();
    }

    @Override
    public Set<K> getAllKeys() {
        return distributedMap.keySet();
    }

    @Override
    public int size() {
        return distributedMap.size();
    }

    @Override
    public void clear() {
        distributedMap.clear();
    }
}
