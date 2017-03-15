package uk.gov.gchq.gaffer.cache;


import java.util.Collection;
import java.util.Set;

public interface ICache <K, V> {

    V get(K key);

    void put(K key, V value);

    Collection<V> getAllValues();

    Set<K> getAllKeys();

    int size();

    void clear();

}
