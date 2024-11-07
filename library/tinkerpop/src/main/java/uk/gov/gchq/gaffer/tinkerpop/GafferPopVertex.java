/*
 * Copyright 2016-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A <code>GafferPopEdge</code> is an {@link GafferPopElement} and {@link Vertex}.
 */
public class GafferPopVertex extends GafferPopElement implements Vertex {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopVertex.class);

    private Map<String, List<VertexProperty>> properties;

    public GafferPopVertex(final String label, final Object id, final GafferPopGraph graph) {
        super(label, id, graph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (properties != null && properties.containsKey(key)) {
            final List<VertexProperty> list = properties.get(key);
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
            throw new UnsupportedOperationException("Updates are not supported, Vertex is readonly");
        }
        // Attach the property to this vertex before updating and re adding to the graph
        VertexProperty<V> vertexProperty = propertyWithoutUpdate(cardinality, key, value, keyValues);
        LOGGER.info("Updating Vertex properties via aggregation");

        // Re add to do a update via aggregation
        graph().addVertex(this);
        return vertexProperty;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return properties == null ?
                Collections.emptyIterator() :
                (Iterator) properties.entrySet()
                        .stream()
                        .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                        .flatMap(entry -> entry.getValue().stream())
                        .iterator();
    }

    /**
     * Updates the properties attached to this Vertex but without modifying the
     * underlying graph.
     *
     * This method is largely a helper for generating GafferPopVertex objects
     * from Gaffer Entities returned from the graph. In this situation we want
     * to be able to create a representative Vertex but without modifying the
     * one stored in the graph.
     *
     * @param <V> Value type
     * @param cardinality The cardinality
     * @param key The property key
     * @param value The property value
     * @param keyValues Additional key value pairs
     * @return The VertexProperty
     */
    public <V> VertexProperty<V> propertyWithoutUpdate(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        // Validate the property to be added
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) {
            return optionalVertexProperty.get();
        }

        final VertexProperty<V> vertexProperty = new GafferPopVertexProperty<>(this, key, value);

        if (null == properties) {
            properties = new HashMap<>();
        }

        final List<VertexProperty> list = properties.getOrDefault(key, new ArrayList<>());
        list.add(vertexProperty);
        properties.put(key, list);
        ElementHelper.attachProperties(vertexProperty, keyValues);

        return vertexProperty;
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) {
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        }

        final GafferPopEdge edge = new GafferPopEdge(label, id, vertex.id(), graph());
        ElementHelper.attachProperties(edge, keyValues);
        graph().addEdge(edge);

        // Check if read only elements
        if (!graph().configuration().containsKey(GafferPopGraph.NOT_READ_ONLY_ELEMENTS)) {
            edge.setReadOnly();
        }

        return edge;
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        // Get edges with matching labels and direction from the graph
        View view = new View.Builder()
            .edges(Arrays.asList(edgeLabels))
            .build();

        return edges(direction, view);
    }

    public Iterator<Edge> edges(final Direction direction, final View view) {
        return graph().edgesWithView(id, direction, view);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return graph().adjVertices(id, direction, edgeLabels);
    }

    public Iterator<Vertex> vertices(final Direction direction, final View view) {
        return graph().adjVerticesWithView(id, direction, view);
    }

    @Override
    public Set<String> keys() {
        if (properties == null) {
            return Collections.emptySet();
        }
        return properties.keySet();
    }

    @Override
    public void remove() {
        // Gaffer does not support deleting elements
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
