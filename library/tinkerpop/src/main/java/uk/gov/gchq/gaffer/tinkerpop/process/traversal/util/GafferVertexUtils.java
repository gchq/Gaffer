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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class GafferVertexUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferVertexUtils.class);

    private GafferVertexUtils() {
        // Utility class
    }

    /**
     * Util method to extract vertices that are vertices on an edge but do not have
     * an
     * associated {@link Vertex} or {@link Entity} the current graph.
     * These vertices exist only on an edge.
     *
     *
     * @param result    The results from a Gaffer query
     * @param graph     The GafferPop graph being queried
     * @param vertexIds The vertexIds that have been used as seeds in the query
     * @return {@link Collection} of 'orphan' {@link Vertex}'s
     */

    public static Collection<Vertex> getOrphanVertices(final Iterable<? extends Element> result, final GafferPopGraph graph, final Object... vertexIds) {
        // Check for any vertex ID seeds that are not returned as Entities
        List<Object> orphanVertexIds = Arrays.stream(vertexIds)
            .filter(id -> StreamSupport.stream(result.spliterator(), false)
                .filter(Entity.class::isInstance)
                .map(e -> ((Entity) e).getVertex())
                .noneMatch(e -> e.equals(id)))
            .collect(Collectors.toList());

        orphanVertexIds.forEach(id -> LOGGER.debug("Getting orphan vertices for vertex {}", id));
        return (orphanVertexIds.isEmpty()) ? Collections.emptyList() : extractOrphanVerticesFromEdges(result, graph, orphanVertexIds);
    }

    /**
     * Extracts vertices from {@link Edge}'s which have not been stored as
     * an {@link Entity} in Gaffer. These will be returned as a 'dummy' {@link Vertex}.
     *
     * @param result The results of a Gaffer query
     * @param graph The GafferPop graph being queried
     * @param orphanVertexIds Any seeds that were not found to have an entity
     * @return {@link Collection} of 'orphan' {@link Vertex}'s
     */
    private static Collection<Vertex> extractOrphanVerticesFromEdges(final Iterable<? extends Element> result, final GafferPopGraph graph, final List<Object> orphanVertexIds) {
    return StreamSupport.stream(result.spliterator(), false)
        .filter(Edge.class::isInstance)
        .map(e -> (Edge) e)
        .map(e -> {
            if (orphanVertexIds.contains(e.getSource()) || orphanVertexIds.equals(e.getSource())) {
                return new GafferPopVertex(GafferPopGraph.ID_LABEL, GafferCustomTypeFactory.parseForGraphSONv3(e.getSource()), graph);
            }
            if (orphanVertexIds.contains(e.getDestination()) || orphanVertexIds.equals(e.getDestination())) {
                return new GafferPopVertex(GafferPopGraph.ID_LABEL, GafferCustomTypeFactory.parseForGraphSONv3(e.getDestination()), graph);
            }
            return e;
        })
        .filter(Vertex.class::isInstance)
        .map(v -> (Vertex) v)
        .collect(Collectors.toSet());
    }
}
