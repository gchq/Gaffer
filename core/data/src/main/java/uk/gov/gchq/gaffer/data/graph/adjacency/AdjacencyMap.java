/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.graph.adjacency;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.data.element.Edge;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

/**
 * An {@code AdjacencyMap} is used to store the contents of a graph in memory in
 * a format which can easily be interrogated.
 */
public class AdjacencyMap {

    /**
     * Backing object used to store the AdjacencyMap representation.
     */
    private final HashBasedTable<Object, Object, Set<Edge>> edgeGraph = HashBasedTable.create();

    /**
     * Get the entries in the AdjacencyMap which match the provided source and
     * destination vertices.
     *
     * @param source      the source vertex
     * @param destination the destination vertex
     *
     * @return the {@link Set} of edge objects relating to the specified
     * vertices
     */
    public Set<Edge> getEdges(final Object source, final Object destination) {
        final Set<Edge> results = edgeGraph.get(source, destination);
        return null != results ? results : Collections.emptySet();
    }

    /**
     * Add an entry to the AdjacencyMap.
     *
     * @param source      the source vertex
     * @param destination the destination vertex
     * @param set         the {@link Set} of edge objects to associate with the
     *                    specified pair of vertices.
     *
     * @return the added edge objects
     */
    public Set<Edge> putEdges(final Object source, final Object destination, final Set<Edge> set) {
        return set.stream()
                .flatMap(v -> putEdge(source, destination, v).stream())
                .collect(toSet());
    }

    /**
     * Add an entry to the AdjacencyMap.
     *
     * @param source      the source vertex
     * @param destination the destination vertex
     * @param edge        the edge to add
     *
     * @return the {@link Set} containing the edge objects associated with the
     * source and destination vertices
     */
    public Set<Edge> putEdge(final Object source, final Object destination, final Edge edge) {
        final Set<Edge> existing = edgeGraph.get(source, destination);
        if (null == existing) {
            final Set<Edge> set = Sets.newHashSet(edge);
            return edgeGraph.put(source, destination, set);
        } else {
            existing.add(edge);
            return existing;
        }
    }

    /**
     * Given a source vertex, get all of the vertices which can be reached from
     * that source.
     *
     * @param source the source vertex
     *
     * @return a {@link Set} of the destination vertices
     */
    public Set<Object> getDestinations(final Object source) {
        return Collections.unmodifiableSet(edgeGraph.row(source).keySet());
    }

    /**
     * Given a destination vertex, get all of the vertices which are linked to
     * that destination.
     *
     * @param destination the destination vertex
     *
     * @return a {@link Set} of the source vertices
     */
    public Set<Object> getSources(final Object destination) {
        return Collections.unmodifiableSet(edgeGraph.column(destination).keySet());
    }

    /**
     * Get a {@link Set} containing all of the source vertices in this
     * AdjacencyMap.
     *
     * @return an immutable set containing the source vertices
     */
    public Set<Object> getAllSources() {
        return Collections.unmodifiableSet(edgeGraph.rowKeySet());
    }

    /**
     * Get a {@link Set} containing all of the destination vertices in this
     * AdjacencyMap.
     *
     * @return an immutable set containing the destination vertices
     */
    public Set<Object> getAllDestinations() {
        return Collections.unmodifiableSet(edgeGraph.columnKeySet());
    }

    /**
     * Given a vertex, remove all entries in the AdjacencyMap which have this
     * vertex as a destination.
     *
     * @param destination the destination vertex
     */
    public void removeAllWithDestination(final Object destination) {
        final Set<Object> set = Sets.newHashSet(getSources(destination));
        for (final Object v : set) {
            edgeGraph.remove(v, destination);
        }
    }

    /**
     * Determines whether this AdjacencyMap contains the specified source
     * vertex.
     *
     * @param source the source vertex to query for
     *
     * @return {@code true} if the adjacency map contains the source, otherwise
     * {@code false}
     */
    public boolean containsSource(final Object source) {
        return edgeGraph.rowKeySet().contains(source);
    }

    /**
     * Determines whether this AdjacencyMap contains the specified destination
     * vertex.
     *
     * @param destination the source vertex to query for
     *
     * @return {@code true} if the adjacency map contains the destination,
     * otherwise {@code false}
     */
    public boolean containsDestination(final Object destination) {
        return edgeGraph.columnKeySet().contains(destination);
    }

    public String toStringFull() {
        return super.toString() + '[' + toString() + ']';
    }

    @Override
    public String toString() {
        return edgeGraph.rowKeySet().stream()
                .map(s -> s.toString() + "->" + getDestinations(s))
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
