/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper class around the Java {@link Map} interface, providing some additional
 * utility methods required by the Gaffer {@link uk.gov.gchq.gaffer.mapstore.MapStore}.
 *
 * @param <K> the type of key in the map
 * @param <V> the type of value in the map
 */
public class MapWrapper<K, V> implements Map<K, V> {
    private final Map<K, V> map;

    public MapWrapper(final Map<K, V> map) {
        if (null == map) {
            throw new IllegalArgumentException("Map cannot be null");
        }
        this.map = map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key) {
        return map.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(final Object value) {
        return map.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(final Object key) {
        return map.get(key);
    }

    /**
     * Given a {@link Set} of keys, retrieve all of the values mapped to those keys.
     *
     * @param keys the keys to search for
     * @return a submap containing all of the key-value pairs matching the input
     * keys
     */
    public Map<K, V> getAll(final Set<K> keys) {
        final Map<K, V> submap = new HashMap<>(keys.size());
        for (final K key : keys) {
            final V value = get(key);
            if (null != value) {
                submap.put(key, value);
            }
        }

        return submap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(final K key, final V value) {
        return map.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(final Object key) {
        return map.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        map.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<V> values() {
        return map.values();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    protected Map<K, V> getMap() {
        return map;
    }
}
