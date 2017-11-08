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

import org.hamcrest.collection.IsCollectionWithSize;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsNot.not;

public class PrunedAdjacencyMapsTest {

    @Test
    public void shouldPrune() {
        // Given
        final AdjacencyMaps<Object, Object> adjacencyMaps = new PrunedAdjacencyMaps<>();

        final AdjacencyMap<Object, Object> first = new AdjacencyMap<>();
        first.put(1, 2, 1);
        first.put(1, 3, 1);

        final AdjacencyMap<Object, Object> second = new AdjacencyMap<>();
        second.put(2, 3, 1);
        second.put(2, 4, 1);

        // There are no edges which follow on from the edge 1->3 in the first
        // adjacency map.

        // When
        adjacencyMaps.add(first);
        adjacencyMaps.add(second);

        // Then
        final AdjacencyMap<Object,Object> firstPruned = adjacencyMaps.get(0);
        final AdjacencyMap<Object,Object> secondPruned = adjacencyMaps.get(1);

        assertThat(firstPruned.getDestinations(1), hasSize(1));
        assertThat(secondPruned.getDestinations(2), hasSize(2));
    }

    @Test
    public void shouldPruneRecursively() {
        // Given
        final AdjacencyMaps<Object, Object> adjacencyMaps = new PrunedAdjacencyMaps<>();

        final AdjacencyMap<Object, Object> first = new AdjacencyMap<>();
        first.put(1, 2, 1);
        first.put(1, 3, 1);

        final AdjacencyMap<Object, Object> second = new AdjacencyMap<>();
        second.put(2, 4, 1);
        second.put(2, 5, 1);
        second.put(3, 6, 1);
        second.put(3, 7, 1);

        final AdjacencyMap<Object, Object> third = new AdjacencyMap<>();
        third.put(4, 8, 1);
        third.put(4, 9, 1);
        third.put(5, 10, 1);
        third.put(5, 11, 1);

        // There are no edges which follow on from the edges 3->6 or 3->7 in the
        // second adjacency map. This should result in all edges which stem from
        // the root edge from 1->3 being removed, including the root edge itself

        // When
        adjacencyMaps.add(first);
        adjacencyMaps.add(second);
        adjacencyMaps.add(third);

        // Then
        final AdjacencyMap<Object,Object> firstPruned = adjacencyMaps.get(0);
        final AdjacencyMap<Object,Object> secondPruned = adjacencyMaps.get(1);
        final AdjacencyMap<Object,Object> thirdPruned = adjacencyMaps.get(2);

        assertThat(firstPruned.getDestinations(1), hasSize(1));
        assertThat(secondPruned.getDestinations(2), hasSize(2));
        assertThat(thirdPruned.getDestinations(4), hasSize(2));
        assertThat(thirdPruned.getDestinations(5), hasSize(2));
    }
}
