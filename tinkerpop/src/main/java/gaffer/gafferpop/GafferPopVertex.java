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

import gaffer.data.elementdefinition.view.View;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A <code>GafferPopEdge</code> is an {@link GafferPopElement} and {link Vertex}.
 */
public class GafferPopVertex extends GafferPopElement implements Vertex {
    private Map<String, List<VertexProperty>> properties;

    public GafferPopVertex(final String label, final Object id, final GafferPopGraph graph) {
        super(label, id, graph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.properties != null && this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() > 1) {
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            } else {
                return list.get(0);
            }
        } else {
            return VertexProperty.<V>empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (isReadOnly()) {
            throw new UnsupportedOperationException("Updates are not supported");
        }

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) {
            return optionalVertexProperty.get();
        }

        final VertexProperty<V> vertexProperty = new GafferPopVertexProperty<>(this, key, value);

        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        final List<VertexProperty> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(vertexProperty);
        this.properties.put(key, list);
        ElementHelper.attachProperties(vertexProperty, keyValues);
        return vertexProperty;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (null == this.properties) {
            return Collections.emptyIterator();
        }
        if (propertyKeys.length == 1) {
            final List<VertexProperty> properties = this.properties.getOrDefault(propertyKeys[0], Collections.emptyList());
            if (properties.size() == 1) {
                return IteratorUtils.of(properties.get(0));
            } else if (properties.isEmpty()) {
                return Collections.emptyIterator();
            } else {
                return (Iterator) new ArrayList<>(properties).iterator();
            }
        } else {
            return (Iterator) this.properties.entrySet()
                    .stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                    .flatMap(entry -> entry.getValue().stream())
                    .collect(Collectors.toList())
                    .iterator();
        }
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) {
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        }

        final GafferPopEdge edge = new GafferPopEdge(label, id, vertex.id(), graph());
        ElementHelper.attachProperties(edge, keyValues);
        graph().addEdge(edge);

        edge.setReadOnly();
        return edge;
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return (Iterator) graph().edges(id, direction, edgeLabels);
    }

    public Iterator<GafferPopEdge> edges(final Direction direction, final View view) {
        return graph().edgesWithView(id, direction, view);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return (Iterator) graph().adjVertices(id, direction, edgeLabels);
    }

    public Iterator<GafferPopVertex> vertices(final Direction direction, final View view) {
        return graph().adjVerticesWithView(id, direction, view);
    }

    @Override
    public Set<String> keys() {
        if (null == this.properties) {
            return Collections.emptySet();
        }
        return this.properties.keySet();
    }

    @Override
    public String toString() {
        return "v[" + label() + "-" + id() + "]";
    }
}
