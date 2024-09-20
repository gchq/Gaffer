/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Custom GafferPop VertexStep.
 * <p>
 * Takes an Iterable of Vertices as input rather than a single Vertex.
 * This is so we can perform a single Gaffer query for multiple vertices rather
 * than one each.
 * <p>
 * There are some consequences to this optimisation, for queries where there are
 * multiple of the same input seed.
 * In Gremlin, you would expect to get multiple of the same result.
 * However, in Gaffer, if you supply multiple of the same seed only one result
 * is returned.
 * <p>
 * e.g. for the Tinkerpop Modern Graph:
 *
 * <pre>
 * (Gremlin)   g.V().out() = [v2, v3, v3, v3, v4, v5]
 * (GafferPop) g.V().out() = [v2, v3, v4, v5]
 * </pre>
 */
public class GafferPopVertexStep<E extends Element> extends FlatMapStep<Iterable<Vertex>, E>
        implements AutoCloseable, Configuring {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopVertexStep.class);

    protected Parameters parameters = new Parameters();
    private final String[] edgeLabels;
    private Direction direction;
    private final Class<E> returnClass;

    public GafferPopVertexStep(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep.getTraversal());
        this.direction = originalVertexStep.getDirection();
        this.edgeLabels = originalVertexStep.getEdgeLabels();
        this.returnClass = originalVertexStep.getReturnClass();
        this.traversal = originalVertexStep.getTraversal();
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Iterable<Vertex>> traverser) {
        return Vertex.class.isAssignableFrom(returnClass) ?
            (Iterator<E>) this.vertices(traverser.get()) :
            (Iterator<E>) this.edges(traverser.get());
    }

    public Direction getDirection() {
        return this.direction;
    }

    public String[] getEdgeLabels() {
        return this.edgeLabels;
    }

    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    public void reverseDirection() {
        this.direction = this.direction.opposite();
    }

    public boolean returnsVertex() {
        return this.returnClass.equals(Vertex.class);
    }

    public boolean returnsEdge() {
        return this.returnClass.equals(Edge.class);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, Arrays.asList(this.edgeLabels),
                this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), direction, returnClass);
        // edgeLabels' order does not matter because in("x", "y") and in("y", "x") must
        // be considered equal.
        if (edgeLabels != null && edgeLabels.length > 0) {
            final List<String> sortedEdgeLabels = Arrays.stream(edgeLabels)
                    .sorted(Comparator.nullsLast(Comparator.naturalOrder())).collect(Collectors.toList());
            for (final String edgeLabel : sortedEdgeLabels) {
                result = 31 * result + Objects.hashCode(edgeLabel);
            }
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other != null
            && other.getClass().equals(this.getClass())
            && this.hashCode() == other.hashCode();
    }

    @Override
    public void close() throws Exception {
        closeIterator();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    private Iterator<Vertex> vertices(final Iterable<Vertex> vertices) {
        GafferPopGraph graph = (GafferPopGraph) getTraversal().getGraph().get();

        if (edgeLabels.length == 0) {
            LOGGER.debug("Getting {} AdjVertices for vertices", direction);
            return graph.adjVertices((Iterable) vertices, direction);
        }

        LOGGER.debug("Getting {} AdjVertices for edges {} for vertices", direction, edgeLabels);
        View view = new View.Builder().edges(Arrays.asList(edgeLabels)).build();
        return graph.adjVerticesWithView((Iterable) vertices, direction, view);
    }

    private Iterator<Edge> edges(final Iterable<Vertex> vertices) {
        GafferPopGraph graph = (GafferPopGraph) getTraversal().getGraph().get();

        if (edgeLabels.length == 0) {
            LOGGER.debug("Getting {} edges for vertices", direction);
            return graph.edges((Iterable) vertices, direction);
        }

        LOGGER.debug("Getting {} edges with labels {} for vertices", direction, edgeLabels);
        View view = new View.Builder().edges(Arrays.asList(edgeLabels)).build();
        return graph.edgesWithView((Iterable) vertices, direction, view);
    }
}
