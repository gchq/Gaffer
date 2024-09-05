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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class GafferPopVertexStep<E extends Element> extends VertexStep<E> {
    private static final long serialVersionUID = 9061271587784526829L;
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopVertexStep.class);
    private final transient GafferPopGraph graph;

    public GafferPopVertexStep(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep.getTraversal(), originalVertexStep.getReturnClass(), originalVertexStep.getDirection(),
                originalVertexStep.getEdgeLabels());
        LOGGER.debug("Running custom VertexStep on GafferPopGraph");

        this.graph = (GafferPopGraph) originalVertexStep.getTraversal().getGraph().get();
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        List<Vertex> v = (List<Vertex>) traverser.get();
        return (Iterator<E>) (Vertex.class.isAssignableFrom(this.getReturnClass()) ? this.vertices(v) : this.edges(v));
    }

    private Iterator<? extends Vertex> vertices(final List<Vertex> vertices) {
        List<Object> vertexIds = vertices.stream().map(Element::id).collect(Collectors.toList());

        String[] edgeLabels = this.getEdgeLabels();
        if (edgeLabels.length == 0) {
            return graph.adjVertices(vertexIds, getDirection());
        }

        View view = new View.Builder().edges(Arrays.asList(edgeLabels)).build();
        return graph.adjVerticesWithView(vertexIds, getDirection(), view);
    }

    private Iterator<? extends Edge> edges(final List<Vertex> vertices) {
        List<Object> vertexIds = vertices.stream().map(Element::id).collect(Collectors.toList());

        String[] edgeLabels = this.getEdgeLabels();
        if (edgeLabels.length == 0) {
            return graph.edges(vertexIds, getDirection());
        }

        View view = new View.Builder().edges(Arrays.asList(getEdgeLabels())).build();
        return graph.edgesWithView(vertexIds, getDirection(), view);
    }
}
