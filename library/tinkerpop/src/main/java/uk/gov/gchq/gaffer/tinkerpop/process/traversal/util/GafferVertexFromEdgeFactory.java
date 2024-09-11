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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.util;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class GafferVertexFromEdgeFactory {
    private static Iterable<Vertex> nonEntityVertices;

    private GafferVertexFromEdgeFactory() {
        // Utility class
    }

    public static Iterable<Vertex> checkForSeed(final Iterable<? extends Element> result, final GafferPopGraph graph, final Object... vertexIds) {
        if (!checkIfSeedInEntityResults(result, vertexIds)) {
            nonEntityVertices = extractSeedMatchFromEdges(result, graph, vertexIds);
        }
        return nonEntityVertices;
    }

    private static boolean checkIfSeedInEntityResults(final Iterable<? extends Element> result, final Object... vertexIds) {
        return StreamSupport.stream(result.spliterator(), false)
            .filter(Entity.class::isInstance)
            .map(e -> ((Entity) e).getVertex())
            .anyMatch(e -> Arrays.asList(vertexIds).contains(e));
    }

    private static Iterable<Vertex> extractSeedMatchFromEdges(final Iterable<? extends Element> result, final GafferPopGraph graph, final Object... vertexIds) {
        return StreamSupport.stream(result.spliterator(), false)
            .filter(Edge.class::isInstance)
            .map(e -> ((Edge) e))
            .map(e -> {
                if (Arrays.asList(vertexIds).contains(e.getDestination())) {
                    return new GafferPopVertex(e.getGroup(), GafferCustomTypeFactory.parseForGraphSONv3(e.getDestination()), graph);
                } else if (Arrays.asList(vertexIds).contains(e.getSource())) {
                    return new GafferPopVertex(e.getGroup(), GafferCustomTypeFactory.parseForGraphSONv3(e.getSource()), graph);
                } else {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        }
}
