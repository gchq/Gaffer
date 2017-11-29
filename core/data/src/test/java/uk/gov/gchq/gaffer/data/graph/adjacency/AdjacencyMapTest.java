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

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;

public class AdjacencyMapTest {

    @Test
    public void shouldGetEdges() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, hasItems(1));
    }

    @Test
    public void shouldGetEmptyEdgeSet() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getEdges(1, 6);

        // Then
        assertThat(results, is(empty()));
    }

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
        final Set<Object> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, equalTo(Collections.singleton(1)));
    }

    @Test
    public void shouldPutMultipleEdges() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = new AdjacencyMap<>();

        adjacencyMap.putEdge(1, 2, 1);
        adjacencyMap.putEdges(1, 2, Sets.newHashSet(2, 3));

        // When
        final Set<Object> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, hasItems(1, 2, 3));
    }

    @Test
    public void shouldPutEdgeWhenExisting() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = new AdjacencyMap<>();

        adjacencyMap.putEdge(1, 2, 1);
        adjacencyMap.putEdge(1, 2, 2);
        adjacencyMap.putEdge(1, 2, 3);

        // When
        final Set<Object> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, hasItems(1, 2, 3));
    }

    @Test
    public void shouldContainDestination() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsDestination(2);

        // Then
        assertThat(result, is(true));
    }

    @Test
    public void shouldNotContainDestination() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsDestination(7);

        // Then
        assertThat(result, is(false));
    }

    @Test
    public void shouldContainSource() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsSource(2);

        // Then
        assertThat(result, is(true));
    }

    @Test
    public void shouldNotContainSource() {
        // Given
        final AdjacencyMap<Object, Object> adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsSource(7);

        // Then
        assertThat(result, is(false));
    }

    private AdjacencyMap<Object, Object> getAdjacencyMap() {
        final AdjacencyMap<Object, Object> adjacencyMap = new AdjacencyMap<>();

        adjacencyMap.putEdge(1, 2, 1);
        adjacencyMap.putEdge(2, 3, 1);
        adjacencyMap.putEdge(6, 3, 1);
        adjacencyMap.putEdge(5, 6, 1);
        adjacencyMap.putEdge(5, 4, 1);
        adjacencyMap.putEdge(4, 1, 1);
        adjacencyMap.putEdge(1, 5, 1);
        adjacencyMap.putEdge(1, 1, 1);

        return adjacencyMap;
    }
}
