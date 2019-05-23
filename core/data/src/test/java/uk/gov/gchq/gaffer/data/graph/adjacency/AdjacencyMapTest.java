/*
 * Copyright 2017-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;

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
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final Set<Edge> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, hasItems(makeEdge(1, 2)));
    }

    @Test
    public void shouldGetEmptyEdgeSet() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final Set<Edge> results = adjacencyMap.getEdges(1, 6);

        // Then
        assertThat(results, is(empty()));
    }

    @Test
    public void shouldGetDestinations() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getDestinations(1);

        // Then
        assertThat(results, hasItems(1, 2, 5));
    }

    @Test
    public void shouldGetAllDestinations() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getAllDestinations();

        // Then
        assertThat(results, hasItems(1, 2, 3, 4, 5, 6));
    }

    @Test
    public void shouldGetSources() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getSources(1);

        // Then
        assertThat(results, hasItems(1, 4));
    }

    @Test
    public void shouldGetAllSources() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final Set<Object> results = adjacencyMap.getAllSources();

        // Then
        assertThat(results, hasItems(1, 2, 4, 5, 6));
    }

    @Test
    public void shouldGetEntry() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final Set<Edge> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, equalTo(Collections.singleton(makeEdge(1, 2))));
    }

    @Test
    public void shouldPutMultipleEdges() {
        // Given
        final AdjacencyMap adjacencyMap = new AdjacencyMap();

        adjacencyMap.putEdge(1, 2, makeEdge(1, 2));
        adjacencyMap.putEdges(1, 2, Sets.newHashSet(makeEdge(TestGroups.EDGE_2, 1, 2), makeEdge(TestGroups.EDGE_3, 1, 2)));

        // When
        final Set<Edge> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, hasItems(makeEdge(1, 2), makeEdge(TestGroups.EDGE_2, 1, 2), makeEdge(TestGroups.EDGE_3, 1, 2)));
    }

    @Test
    public void shouldPutEdgeWhenExisting() {
        // Given
        final AdjacencyMap adjacencyMap = new AdjacencyMap();

        adjacencyMap.putEdge(1, 2, makeEdge(1, 2));
        adjacencyMap.putEdge(1, 2, makeEdge(TestGroups.EDGE_2, 1, 2));
        adjacencyMap.putEdge(1, 2, makeEdge(TestGroups.EDGE_3, 1, 2));

        // When
        final Set<Edge> results = adjacencyMap.getEdges(1, 2);

        // Then
        assertThat(results, hasItems(makeEdge(1, 2), makeEdge(TestGroups.EDGE_2, 1, 2), makeEdge(TestGroups.EDGE_3, 1, 2)));
    }

    @Test
    public void shouldContainDestination() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsDestination(2);

        // Then
        assertThat(result, is(true));
    }

    @Test
    public void shouldNotContainDestination() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsDestination(7);

        // Then
        assertThat(result, is(false));
    }

    @Test
    public void shouldContainSource() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsSource(2);

        // Then
        assertThat(result, is(true));
    }

    @Test
    public void shouldNotContainSource() {
        // Given
        final AdjacencyMap adjacencyMap = getAdjacencyMap();

        // When
        final boolean result = adjacencyMap.containsSource(7);

        // Then
        assertThat(result, is(false));
    }

    private AdjacencyMap getAdjacencyMap() {
        final AdjacencyMap adjacencyMap = new AdjacencyMap();

        adjacencyMap.putEdge(1, 2, makeEdge(1, 2));
        adjacencyMap.putEdge(2, 3, makeEdge(2, 3));
        adjacencyMap.putEdge(6, 3, makeEdge(6, 3));
        adjacencyMap.putEdge(5, 6, makeEdge(5, 6));
        adjacencyMap.putEdge(5, 4, makeEdge(5, 4));
        adjacencyMap.putEdge(4, 1, makeEdge(4, 1));
        adjacencyMap.putEdge(1, 5, makeEdge(1, 5));
        adjacencyMap.putEdge(1, 1, makeEdge(1, 1));

        return adjacencyMap;
    }

    private Edge makeEdge(final Object source, final Object destination) {
        return makeEdge(TestGroups.EDGE, source, destination);
    }

    private Edge makeEdge(final String group, final Object source, final Object destination) {
        return new Edge.Builder().group(TestGroups.EDGE).source(source).dest(destination).directed(true).build();
    }
}
