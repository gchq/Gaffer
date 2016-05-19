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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A <code>GafferPopEdge</code> is an {@link GafferPopElement} and {link Edge}.
 * <p>
 * inVertex() and outVertex() methods are not supported as it is possible for a
 * edge to have multiple in vertices and multiple out vertices (due to the mapping
 * to a TinkerPop vertex to Gaffer {@link gaffer.data.element.Entity}.
 * Use vertices(Direction) instead.
 * </p>
 * <p>
 * An ID is required to be an {@link EdgeId} which contains the source and
 * destination of an edge.
 * </p>
 */
public final class GafferPopEdge extends GafferPopElement implements Edge {
    private Map<String, Property> properties;

    public GafferPopEdge(final String label, final Object source, final Object dest, final GafferPopGraph graph) {
        super(label, new EdgeId(source, dest), graph);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        if (isReadOnly()) {
            throw new UnsupportedOperationException("Updates are not supported");
        }
        ElementHelper.validateProperty(key, value);
        final Property<V> newProperty = new GafferPopProperty<>(this, key, value);
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, newProperty);
        return newProperty;

    }

    @Override
    public <V> Property<V> property(final String key) {
        return null == this.properties ? Property.<V>empty() : this.properties.getOrDefault(key, Property.<V>empty());
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        if (null == this.properties) {
            return Collections.emptyIterator();
        }
        if (propertyKeys.length == 1) {
            final Property<V> property = this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else {
            return (Iterator) this.properties.entrySet()
                    .stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                    .map(entry -> entry.getValue()).collect(Collectors.toList())
                    .iterator();
        }
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (Direction.IN.equals(direction)) {
            return graph().vertices(id().getDest());
        }

        if (Direction.OUT.equals(direction)) {
            return graph().vertices(id().getSource());
        }
        return graph().vertices(id().getSource(), id().getDest());
    }

    @Override
    public Set<String> keys() {
        return null == this.properties ? Collections.emptySet() : this.properties.keySet();
    }

    @Override
    public EdgeId id() {
        return (EdgeId) super.id();
    }

    @Override
    public String toString() {
        return "e[" + id().getSource() + "-" + label + "->" + id().getDest() + "]";
    }

    @Override
    public Vertex outVertex() {
        throw new UnsupportedOperationException("Use 'vertices(Direction)' instead - it may return multiple vertices");
    }

    @Override
    public Vertex inVertex() {
        throw new UnsupportedOperationException("Use 'vertices(Direction)' instead - it may return multiple vertices");
    }
}
