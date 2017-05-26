/*
 * Copyright 2017 Crown Copyright
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapOfSets<K, V> implements MultiMap<K, V> {
    private final Map<K, Set<V>> multiMap;

    public MapOfSets(final Map<K, Set<V>> multiMap) {
        this.multiMap = multiMap;
    }

    @Override
    public boolean put(final K key, final V value) {
        Set<V> values = multiMap.get(key);
        if (null == values) {
            values = new HashSet<>();
            multiMap.put(key, values);
        }
        return values.add(value);
    }

    @Override
    public Collection<V> get(final K key) {
        return multiMap.get(key);
    }

    @Override
    public void clear() {
        multiMap.clear();
    }
}
