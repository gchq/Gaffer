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
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferPopElementGenerator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A <code>GafferPopEdge</code> is an {@link GafferPopElement} and {@link Edge}.
 * <p>
 * An ID is required to be a List which contains the source, group, and
 * destination of an edge.
 * </p>
 */
public final class GafferPopEdge extends GafferPopElement implements Edge {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopEdge.class);

    private Map<String, Property> properties;
    private final GafferPopVertex inVertex;
    private final GafferPopVertex outVertex;

    public GafferPopEdge(final String label, final Object outVertex, final Object inVertex,
            final GafferPopGraph graph) {
        super(label, Arrays.asList(getVertexId(outVertex), label, getVertexId(inVertex)), graph);
        this.outVertex = getValidVertex(outVertex, graph);
        this.inVertex = getValidVertex(inVertex, graph);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        if (isReadOnly()) {
            throw new UnsupportedOperationException("Updates are not supported, Edge is readonly");
        }

        // Attach properties before updating the graph
        final Property<V> newProperty = propertyWithoutUpdate(key, value);
        LOGGER.info("Updating Edge properties via aggregation");

        // Re add to do the update via aggregation
        graph().addEdge(this);
        return newProperty;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return null == properties ? Property.<V>empty() : properties.getOrDefault(key, Property.<V>empty());
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        if (properties == null) {
            return Collections.emptyIterator();
        }
        if (propertyKeys.length == 1) {
            final Property<V> property = properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else {
            return (Iterator) properties.entrySet()
                    .stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                    .map(entry -> entry.getValue()).collect(Collectors.toList())
                    .iterator();
        }
    }

    /**
     * Updates the properties attached to this Edge but without modifying the
     * underlying graph.
     *
     * This method is largely a helper for generating GafferPopEdge objects from
     * Gaffer Edge returned from the graph. In this situation we want to be
     * able to create a representative GafferPopEdge but without modifying the
     * one stored in the graph.
     *
     * @param <V>   Value type
     * @param key   The key
     * @param value The value
     * @return The property
     */
    public <V> Property<V> propertyWithoutUpdate(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        final Property<V> newProperty = new GafferPopProperty<>(this, key, value);
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(key, newProperty);

        return newProperty;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(outVertex());
            case IN:
                return IteratorUtils.of(inVertex());
            default:
                return IteratorUtils.of(outVertex(), inVertex());
        }
    }

    @Override
    public Set<String> keys() {
        return properties == null ? Collections.emptySet() : properties.keySet();
    }

    @Override
    public void remove() {
        // Gaffer does not support deleting elements
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Vertex outVertex() {
        return getVertex(outVertex);
    }

    @Override
    public Vertex inVertex() {
        return getVertex(inVertex);
    }

    /**
     * Gets the vertex ID object from the supplied vertex.
     *
     * Will check if the supplied Object implements the {@link Vertex} interface
     * if so will pull it from the instance otherwise assumes the Object itself
     * is the ID.
     *
     * @param vertex The vertex Object or ID.
     * @return The ID for the vertex.
     */
    private static Object getVertexId(final Object vertex) {
        // Check if we need to pull the ID from the vertex or can use it directly
        if (vertex instanceof Vertex) {
            return ((GafferPopVertex) vertex).id();
        } else {
            return vertex;
        }
    }

    /**
     * Determines the GafferPopVertex object to connect this GafferPopEdge on.
     *
     * @param vertex The vertex object or ID
     * @param graph  The graph
     * @return A valid GafferPopVertex based on the supplied object or ID.
     */
    private static GafferPopVertex getValidVertex(final Object vertex, final GafferPopGraph graph) {
        if (vertex instanceof Vertex) {
            return (GafferPopVertex) vertex;
        }

        // As a fallback assume its the vertex ID object and construct with the ID
        // label.
        return new GafferPopVertex(GafferPopGraph.ID_LABEL, vertex, graph);
    }

    /**
     * Runs a search to determine the correct Entity to use when a vertex ID
     * is supplied to an edge. Note that this may slow performance.
     *
     * @param vertex The vertex object or ID
     * @return A valid Vertex based on the supplied object or ID.
     */
    private Vertex getVertex(final GafferPopVertex vertex) {
        OperationChain<Iterable<? extends Element>> findBasedOnID = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(vertex.id()))
                        .view(new View.Builder().allEntities(true).build())
                        .build())
                .build();

        Iterable<? extends Element> result = graph().execute(findBasedOnID);

        final GafferPopElementGenerator generator = new GafferPopElementGenerator(graph());

        Optional<Vertex> foundEntity = StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Vertex.class::isInstance)
                .map(e -> (Vertex) e)
                .findFirst();

        if (foundEntity.isPresent()) {
            return foundEntity.get();
        }

        return vertex;
    }

}
