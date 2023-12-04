/*
 * Copyright 2016-2023 Crown Copyright
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A <code>GafferPopEdge</code> is an {@link GafferPopElement} and {@link Edge}.
 * <p>
 * inVertex() and outVertex() methods are not supported as it is possible for a
 * edge to have multiple in vertices and multiple out vertices (due to the mapping
 * to a TinkerPop vertex to Gaffer {@link uk.gov.gchq.gaffer.data.element.Entity}.
 * Use vertices(Direction) instead.
 * </p>
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

    public GafferPopEdge(final String label, final Object outVertex, final Object inVertex, final GafferPopGraph graph) {
        super(label, Arrays.asList(getVertexId(outVertex), getVertexId(inVertex)), graph);
        this.outVertex = getValidVertex(outVertex, graph);
        this.inVertex = getValidVertex(inVertex, graph);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        LOGGER.warn("Updating Edge properties via aggregation");

        ElementHelper.validateProperty(key, value);
        final Property<V> newProperty = new GafferPopProperty<>(this, key, value);
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, newProperty);

        // Re add to do the update via aggregation
        this.graph().addEdge(this);
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
        switch (direction) {
            case OUT:
                return IteratorUtils.of(outVertex);
            case IN:
                return IteratorUtils.of(inVertex);
            default:
                return IteratorUtils.of(outVertex, inVertex);
        }
    }

    @Override
    public Set<String> keys() {
        return null == this.properties ? Collections.emptySet() : this.properties.keySet();
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Vertex outVertex() {
        return outVertex;
    }

    @Override
    public Vertex inVertex() {
        return inVertex;
    }

    /**
     * Gets the vertex ID object from the supplied vertex.
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
     * @param graph The graph
     * @return A valid GafferPopVertex based on the supplied object or ID.
     */
    private static GafferPopVertex getValidVertex(final Object vertex, final GafferPopGraph graph) {
        // Determine if we can cast the vertex object we have been supplied
        if (vertex instanceof Vertex) {
            return (GafferPopVertex) vertex;
        }

        // As a fallback assume its the vertex ID object and construct with the ID label.
        return new GafferPopVertex(GafferPopGraph.ID_LABEL, vertex, graph);

        /*
         * TODO: Review whether a search should be carried out to determine the correct
         *       Entity to use to construct the GafferPopVertex to add this edge too.
         *       Currently a default label will be used if a vertex ID is given to this
         *       method which may result in an incorrect mapping of a GafferPop
         *       'label' to a Gaffer 'group'.
         *
         * A basic example of a search is given below:
         *
         * OperationChain<Iterable<? extends Element>> findBasedOnID = new OperationChain.Builder()
         *     .first(new GetElements.Builder().input(new EntitySeed(vertex)).build())
         *     .build();
         *
         * Iterable<? extends Element> result = graph.execute(findBasedOnID);
         * Object foundEntity = StreamSupport.stream(result.spliterator(), false)
         *     .filter(Entity.class::isInstance)
         *     .findFirst()
         *     .get();
         *
         * return new GafferPopVertexGenerator(graph)._apply((Element) foundEntity);
         */
    }

}
