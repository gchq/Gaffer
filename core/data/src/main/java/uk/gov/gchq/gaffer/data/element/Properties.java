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

package uk.gov.gchq.gaffer.data.element;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * <code>Properties</code> simply extends {@link java.util.HashMap} with property names (String) as keys and property value (Object) as values.
 */
public class Properties extends HashMap<String, Object> {
    private static final long serialVersionUID = -5412533432398907359L;

    public Properties() {
        super();
    }

    public Properties(final Map<String, Object> properties) {
        super(properties);
    }

    public Properties(final String name, final Object property) {
        super();
        put(name, property);
    }

    @Override
    public Object put(final String name, final Object value) {
        if (null != name && null != value) {
            return super.put(name, value);
        }

        return null;
    }

    @Override
    public Properties clone() {
        return new Properties((Map<String, Object>) super.clone());
    }

    /**
     * Removes all properties with names that are not in the provided set.
     *
     * @param propertiesToKeep a set of properties to keep
     */
    public void keepOnly(final Collection<String> propertiesToKeep) {
        final Iterator<Map.Entry<String, Object>> it = entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            if (!propertiesToKeep.contains(entry.getKey())) {
                it.remove();
            }
        }
    }

    public void remove(final Collection<String> propertiesToRemove) {
        if (null != propertiesToRemove) {
            for (final String property : propertiesToRemove) {
                remove(property);
            }
        }
    }

    @Override
    public String toString() {
        final Iterator<Map.Entry<String, Object>> iter = entrySet().iterator();
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        while (iter.hasNext()) {
            final Map.Entry<String, Object> e = iter.next();
            sb.append(e.getKey());
            sb.append("=");
            if (null != e.getValue()) {
                sb.append("<");
                sb.append(e.getValue().getClass().getCanonicalName());
                sb.append(">");
            }
            sb.append(e.getValue());
            if (iter.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.append('}').toString();
    }
}
