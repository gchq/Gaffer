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

package uk.gov.gchq.gaffer.mapstore.multimap;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapOfSets<K, V> implements MultiMap<K, V> {
    private final Map<K, Set<V>> multiMap;

    /**
     * The type of Set to use.
     * If null then a {@link HashSet} will be used.
     */
    private final Class<? extends Set> setClass;

    public MapOfSets(final Map<K, Set<V>> multiMap) {
        this(multiMap, null);
    }

    public MapOfSets(final Map<K, Set<V>> multiMap, final Class<? extends Set> setClass) {
        this.multiMap = multiMap;
        this.setClass = setClass;
    }

    @Override
    public boolean put(final K key, final V value) {
        final Set<V> values = multiMap.computeIfAbsent(key, k -> createSet());
        return values.add(value);
    }

    @Override
    public void put(final K key, final Collection<V> value) {
        final Set<V> existingValue = multiMap.get(key);
        if (null == existingValue) {
            if (value instanceof Set) {
                multiMap.put(key, ((Set) value));
            } else {
                multiMap.put(key, Sets.newHashSet(value));
            }
        } else {
            existingValue.addAll(value);
        }
    }

    @Override
    public Collection<V> get(final K key) {
        return multiMap.get(key);
    }

    @Override
    public Set<K> keySet() {
        return multiMap.keySet();
    }

    public Collection<Map.Entry<K, Set<V>>> entrySet() {
        return multiMap.entrySet();
    }

    @Override
    public void clear() {
        multiMap.clear();
    }

    protected Set<V> createSet() {
        final Set<V> values;
        if (null == setClass) {
            values = new HashSet<>();
        } else {
            try {
                values = setClass.newInstance();
            } catch (final InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException("Unable to create new instance of set " + setClass.getName(), e);
            }
        }
        return values;
    }

    public Map<K, Set<V>> getWrappedMap() {
        return multiMap;
    }
}
