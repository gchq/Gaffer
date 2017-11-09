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

package uk.gov.gchq.gaffer.data.graph.adjacency;

import java.util.Iterator;
import java.util.List;

/**
 * An {@code AdjacencyMaps} object contains a number of {@link AdjacencyMap}
 * objects and can be used to represent the changes in an AdjacencyMap over time
 * or to track the adjacency components of a graph over some other metric.
 *
 * @param <T> the type of object representing the vertices
 * @param <U> the type of object representing the edges
 */
public interface AdjacencyMaps<T, U> extends Iterable<AdjacencyMap<T, U>> {

    /**
     * Add a new {@link AdjacencyMap}.
     *
     * @param adjacencyMap the AdjacencyMap to add
     */
    default void add(final AdjacencyMap<T, U> adjacencyMap) {
        asList().add(adjacencyMap);
    }

    /**
     * Retrieve the nth {@link AdjacencyMap}.
     *
     * @param n the index of the adjacency map to retrieve
     *
     * @return the nth AdjacencyMap
     */
    default AdjacencyMap<T, U> get(final int n) {
        return asList().get(n);
    }

    /**
     * Return the number of {@link AdjacencyMaps} present in the AdjacencyMaps
     * object.
     * <p>
     * Depending on the context, this could refer to the number of hops present,
     * or the number of timesteps etc.
     *
     * @return the size of the AdjacencyMaps object
     */
    default int size() {
        return asList().size();
    }

    @Override
    default Iterator<AdjacencyMap<T, U>> iterator() {
        return asImmutableList().iterator();
    }

    /**
     * Get a representation of the current AdjacencyMaps object as a {@link
     * List}.
     *
     * @return a {@link List} representation of the current AdjacencyMaps object
     */
    List<AdjacencyMap<T, U>> asList();

    /**
     * Get a representation of the current AdjacencyMaps object as an immutable
     * {@link List}.
     *
     * @return a immutable {@link List} representation of the current
     * AdjacencyMaps object
     */
    List<AdjacencyMap<T, U>> asImmutableList();
}
