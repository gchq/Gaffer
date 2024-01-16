package uk.gov.gchq.gaffer.store;

import static java.util.Collections.emptySet;

import java.util.Collection;
import java.util.Set;
import java.util.Collections;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

public class BrokenICache<K, V> implements ICache<K, V>{

    @Override
    public Object get(Object key) {
        return null;
    }

    @Override
    public void put(Object key, Object value) throws CacheOperationException {
        throw new CacheOperationException("Stubbed class");
    }

    @Override
    public void remove(Object key) {
    }

    @Override
    public Collection getAllValues() {
        return Collections.emptyList();
    }

    @Override
    public Set getAllKeys() {
        return emptySet();
    }

    @Override
    public void clear() throws CacheOperationException {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'clear'");
    }

}
