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
package gaffer.gafferpop;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GafferPopVertexProperty<V> extends GafferPopElement implements VertexProperty<V> {
    protected Map<String, Property> properties;
    private final GafferPopVertex vertex;
    private final String key;
    private final V value;

    public GafferPopVertexProperty(final GafferPopVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(vertex.label(), vertex.id(), vertex.graph());
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((org.apache.tinkerpop.gremlin.structure.Element) this);
    }

    @Override
    public Set<String> keys() {
        return null == properties ? Collections.emptySet() : properties.keySet();
    }

    @Override
    public <U> Property<U> property(final String key) {
        return null == properties ? Property.<U>empty() : properties.getOrDefault(key, Property.<U>empty());
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        if (isReadOnly() || vertex.isReadOnly()) {
            throw new UnsupportedOperationException("Updates are not supported");
        }

        final Property<U> property = new GafferPopProperty<>(this, key, value);
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(key, property);
        return property;
    }

    @Override
    public GafferPopVertex element() {
        return vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        if (null == properties) {
            return Collections.emptyIterator();
        }

        if (propertyKeys.length == 1) {
            final Property<U> property = properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        }

        return (Iterator<Property<U>>) (Iterator) properties.entrySet()
                .stream()
                .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                .map(entry -> entry.getValue())
                .collect(Collectors.toList())
                .iterator();
    }
}
