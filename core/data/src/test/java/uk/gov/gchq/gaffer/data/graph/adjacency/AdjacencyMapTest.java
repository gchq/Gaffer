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

import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertNull;

public class AdjacencyMapTest {

    @Test
    public void shouldGetDestinations() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getDestinations(1);

        // Then
        assertThat(results, hasItems(1, 2, 5));
    }

    @Test
    public void shouldGetAllDestinations() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getAllDestinations();

        // Then
        assertThat(results, hasItems(1, 2, 3, 4, 5, 6));
    }

    @Test
    public void shouldGetSources() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getSources(1);

        // Then
        assertThat(results, hasItems(1, 4));
    }

    @Test
    public void shouldGetAllSources() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getAllSources();

        // Then
        assertThat(results, hasItems(1, 2, 4, 5, 6));
    }

    @Test
    public void shouldGetEntry() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.get(1, 2);

        // Then
        assertThat(results, equalTo(Collections.singleton(1)));
    }

    @Test
    public void shouldReturnNullWhenEntryDoesNotExist() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.get(1, 3);

        // Then
        assertNull(results);
    }

    private AdjacencyMap<Object, Object> getAdjacencyMap() {
        final AdjacencyMap<Object, Object> adjacencyMap = new AdjacencyMap<>();
        adjacencyMap.put(1, 2, 1);
        adjacencyMap.put(2, 3, 1);
        adjacencyMap.put(6, 3, 1);
        adjacencyMap.put(5, 6, 1);
        adjacencyMap.put(5, 4, 1);
        adjacencyMap.put(4, 1, 1);
        adjacencyMap.put(1, 5, 1);
        adjacencyMap.put(1, 1, 1);

        return adjacencyMap;
    }
}
