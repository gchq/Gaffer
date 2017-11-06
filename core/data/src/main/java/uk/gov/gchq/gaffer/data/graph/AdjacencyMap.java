/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.data.graph;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * An {@code AdjacencyMap} is used to store the contents of a graph in memory in
 * a format which can easily be interrogated.
 *
 * @param <T> the type of object representing the vertices
 * @param <U> the type of object representing the edge
 */
public class AdjacencyMap<T, U> {

    private final HashBasedTable<T, T, Set<U>> graph = HashBasedTable.create();

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
    public Set<U> get(final T source, final T destination) {
        return graph.get(source, destination);
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
    public Set<U> put(final T source, final T destination, final Set<U> set) {
        return graph.put(source, destination, set);
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
    public Set<U> put(final T source, final T destination, final U edge) {
        final Set<U> existing = graph.get(source, destination);
        if (null == existing) {
            final Set<U> set = Sets.newHashSet(edge);
            return graph.put(source, destination, set);
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
    public Set<T> getDestinations(final T source) {
        return graph.row(source).keySet();
    }

    /**
     * Given a destination vertex, get all of the vertices which are linked to
     * that destination.
     *
     * @param destination the destination vertex
     *
     * @return a {@link Set} of the source vertices
     */
    public Set<T> getSources(final T destination) {
        return graph.column(destination).keySet();
    }

}
