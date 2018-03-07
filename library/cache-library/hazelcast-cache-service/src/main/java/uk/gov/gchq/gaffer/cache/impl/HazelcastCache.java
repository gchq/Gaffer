/*
 * Copyright 2016-2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.cache.impl;

import com.hazelcast.core.IMap;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import java.util.Collection;
import java.util.Set;

/**
 * Implementation of the {@link ICache} interface, using a Hazelcast {@link IMap}
 * as the cache data store.
 *
 * @param <K> The object type that acts as the key for the IMap
 * @param <V> The value that is stored in the IMap
 */
public class HazelcastCache <K, V> implements ICache<K, V> {
    private final IMap<K, V> distributedMap;

    public HazelcastCache(final IMap <K, V> distributedMap) {
        this.distributedMap = distributedMap;
    }

    @Override
    public V get(final K key) {
        return distributedMap.get(key);
    }

    @Override
    public void put(final K key, final V value) throws CacheOperationException {
        try {
            distributedMap.put(key, value);
        } catch (final Exception e) {
            throw new CacheOperationException(e);
        }
    }

    @Override
    public void remove(final K key) {
        distributedMap.remove(key);
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
    public void clear() throws CacheOperationException {
        try {
            distributedMap.clear();
        } catch (final Exception e) {
            throw new CacheOperationException(e);
        }
    }
}
