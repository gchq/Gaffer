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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * <code>LazyProperties</code> wraps {@link Properties} and lazily loads property values when
 * requested using a provided {@link uk.gov.gchq.gaffer.data.element.ElementValueLoader}.
 */
public class LazyProperties extends Properties {
    private static final long serialVersionUID = 9009552236887934877L;
    private final ElementValueLoader valueLoader;
    private final Set<String> loadedProperties;
    private final Properties properties;

    /**
     * Constructs a {@link LazyProperties} by wrapping the provided {@link Properties}
     * and using the {@link uk.gov.gchq.gaffer.data.element.ElementValueLoader} to lazily load the property values when requested.
     *
     * @param properties  the properties to wrap.
     * @param valueLoader the element value loader to use to lazily load the property values
     */
    public LazyProperties(final Properties properties, final ElementValueLoader valueLoader) {
        this.valueLoader = valueLoader;
        this.properties = properties;
        loadedProperties = new HashSet<>(properties.keySet());
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public Object put(final String name, final Object property) {
        final Object result = properties.put(name, property);
        loadedProperties.add(name);

        return result;
    }

    @Override
    public void putAll(final Map<? extends String, ?> newProperties) {
        properties.putAll(newProperties);
        loadedProperties.addAll(newProperties.keySet());
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Doesn't use any properties in super class")
    @Override
    public LazyProperties clone() {
        return new LazyProperties(properties.clone(), valueLoader);
    }

    @Override
    public Object get(final Object name) {
        return get(name.toString());
    }

    public Object get(final String name) {
        final Object value;
        if (loadedProperties.contains(name)) {
            value = properties.get(name);
        } else {
            value = valueLoader.getProperty(name);
            put(name, value);
        }
        return value;
    }

    @Override
    public void clear() {
        properties.clear();
        loadedProperties.clear();
    }

    @Override
    public Object remove(final Object name) {
        final Object result = properties.remove(name);
        loadedProperties.remove(name.toString());
        return result;
    }

    @Override
    public void keepOnly(final Collection<String> propertiesToKeep) {
        properties.keepOnly(propertiesToKeep);
        Iterator<String> it = loadedProperties.iterator();
        while (it.hasNext()) {
            if (!propertiesToKeep.contains(it.next())) {
                it.remove();
            }
        }
    }

    @Override
    public void remove(final Collection<String> propertiesToRemove) {
        properties.remove(propertiesToRemove);
        loadedProperties.removeAll(propertiesToRemove);
    }

    @Override
    public int size() {
        return properties.size();
    }

    @Override
    public boolean isEmpty() {
        return properties.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return properties.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return properties.containsValue(value);
    }

    @Override
    public Set<String> keySet() {
        return properties.keySet();
    }

    @Override
    public Collection<Object> values() {
        return properties.values();
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return properties.entrySet();
    }

    @SuppressWarnings(value = "EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object o) {
        return properties.equals(o);
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }

    @Override
    public String toString() {
        return properties.toString();
    }
}
