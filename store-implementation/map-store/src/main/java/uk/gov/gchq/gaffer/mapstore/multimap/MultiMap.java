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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Map-like data structure where keys may correspond to multiple values.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public interface MultiMap<K, V> {

    /**
     * Add a value to the specified key.
     *
     * @param key the key to add a value to
     * @param value the value to add
     * @return true if the operation was successful, otherwise false
     */
    boolean put(final K key, final V value);

    /**
     * Add a collection of values to the specified key.
     *
     * @param key the key to add the values to
     * @param values the values to add
     */
    void put(final K key, final Collection<V> values);

    /**
     * Get all of the values associated with the specified key.
     *
     * @param key the key to lookup
     * @return a collection containing all of the values
     */
    Collection<V> get(final K key);

    /**
     * Get the Set containing all keys in the map.
     *
     * @return the set of keys
     */
    Set<K> keySet();

    /**
     * Clear the map of all entries.
     */
    void clear();

    /**
     * Add all of the contents of another {@link MultiMap} to this instance.
     *
     * @param map the map containing the entries to add to this map instance
     */
    default void putAll(final MultiMap<K, V> map) {
        if (map instanceof MapOfSets) {
            for (final Map.Entry<K, Set<V>> entry : ((MapOfSets<K, V>) map).entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        } else {
            for (final K key : map.keySet()) {
                put(key, map.get(key));
            }
        }
    }
}
