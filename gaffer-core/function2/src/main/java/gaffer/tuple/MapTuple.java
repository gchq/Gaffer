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

package gaffer.tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A <code>MapTuple</code> is an implementation of {@link gaffer.tuple.Tuple} backed by a
 * {@link java.util.Map}.
 */
public class MapTuple implements Tuple<String> {
    private Map<String, Object> values;

    /**
     * Create a <code>MapTuple</code> backed by the given {@link java.util.Map}.
     * @param values Backing {@link java.util.Map}.
     */
    public MapTuple(final Map<String, Object> values) {
        this.values = values;
    }

    /**
     * Create a <code>MapTuple</code> backed by a new {@link java.util.HashMap}.
     */
    public MapTuple() {
        this.values = new HashMap<String, Object>();
    }

    @Override
    public void put(final String reference, final Object value) {
        values.put(reference, value);
    }

    @Override
    public Object get(final String reference) {
        return values.get(reference);
    }

    @Override
    public Iterable<Object> values() {
        return values.values();
    }

    @Override
    public Iterator<Object> iterator() {
        return values().iterator();
    }
}
