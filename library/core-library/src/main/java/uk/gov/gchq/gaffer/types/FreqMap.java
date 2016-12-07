/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.types;

import java.util.HashMap;
import java.util.Map;

/**
 * <code>FreqMap</code> extends {@link HashMap} with String keys and Long values, adding an upsert operation.
 */
public class FreqMap extends HashMap<String, Long> {
    private static final long serialVersionUID = -851105369975081220L;

    public FreqMap(final Map<? extends String, ? extends Long> m) {
        super(m);
    }

    public FreqMap() {
    }

    public FreqMap(final int initialCapacity) {
        super(initialCapacity);
    }

    public FreqMap(final int initialCapacity, final float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Adds a new key and value to the map if the key is not already there.
     * If the key is already there, the value supplied is added to the existing value for the key and the result is inserted into the map.
     *
     * @param key   The key in the map to increment or insert.
     * @param value The value to increment by or initialise to.
     */
    public void upsert(final String key, final Long value) {
        final Long currentValue = get(key);
        if (null == currentValue) {
            put(key, value);
        } else {
            put(key, currentValue + value);
        }
    }

    /**
     * Increments the value of an existing key by 1.
     * If the key doesn't exist, initialises the value to 1.
     *
     * @param key The key to increment or insert.
     */
    public void upsert(final String key) {
        upsert(key, 1L);
    }
}
