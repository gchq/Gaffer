package uk.gov.gchq.gaffer.cache.impl;


import uk.gov.gchq.gaffer.cache.ICache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

public class HashMapCache <K, V> implements ICache <K, V> {

    private HashMap<K, V> cache = new HashMap<>();

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public Collection<V> getAllValues() {
        return cache.values();
    }

    @Override
    public Set<K> getAllKeys() {
        return cache.keySet();
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
