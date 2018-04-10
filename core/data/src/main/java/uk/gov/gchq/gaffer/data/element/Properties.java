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

package uk.gov.gchq.gaffer.data.element;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code Properties} simply extends {@link java.util.HashMap} with property names (String) as keys and property value (Object) as values.
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
        if (null != name) {
            if (null == value) {
                return super.remove(name);
            } else {
                return super.put(name, value);
            }
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
        entrySet().removeIf(entry -> !propertiesToKeep.contains(entry.getKey()));
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
        final ToStringBuilder sb = new ToStringBuilder(this);
        super.forEach((key, value) -> sb.append(key, String.format("<%s>%s", value.getClass().getCanonicalName(), value)));
        return sb.build();
    }
}
