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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class PrunedAdjacencyMapsTest {

    @Test
    public void shouldPrune() {
        // Given
        final AdjacencyMaps adjacencyMaps = new PrunedAdjacencyMaps();

        final AdjacencyMap first = new AdjacencyMap();
        first.putEdge(1, 2, makeEdge(1, 2));
        first.putEdge(1, 3, makeEdge(1, 3));

        final AdjacencyMap second = new AdjacencyMap();
        second.putEdge(2, 3, makeEdge(2, 3));
        second.putEdge(2, 4, makeEdge(2, 4));

        // There are no edges which follow on from the edge 1->3 in the first
        // adjacency map.

        // When
        adjacencyMaps.add(first);
        adjacencyMaps.add(second);

        // Then
        final AdjacencyMap firstPruned = adjacencyMaps.get(0);
        final AdjacencyMap secondPruned = adjacencyMaps.get(1);

        assertThat(firstPruned.getDestinations(1), hasSize(1));
        assertThat(secondPruned.getDestinations(2), hasSize(2));
    }

    @Test
    public void shouldPruneRecursively() {
        // Given
        final AdjacencyMaps adjacencyMaps = new PrunedAdjacencyMaps();

        final AdjacencyMap first = new AdjacencyMap();
        first.putEdge(1, 2, makeEdge(1, 2));
        first.putEdge(1, 3, makeEdge(1, 3));

        final AdjacencyMap second = new AdjacencyMap();
        second.putEdge(2, 4, makeEdge(2, 4));
        second.putEdge(2, 5, makeEdge(2, 5));
        second.putEdge(3, 6, makeEdge(3, 6));
        second.putEdge(3, 7, makeEdge(3, 7));

        final AdjacencyMap third = new AdjacencyMap();
        third.putEdge(4, 8, makeEdge(4, 8));
        third.putEdge(4, 9, makeEdge(4, 9));
        third.putEdge(5, 10, makeEdge(5, 10));
        third.putEdge(5, 11, makeEdge(5, 11));

        // There are no edges which follow on from the edges 3->6 or 3->7 in the
        // second adjacency map. This should result in all edges which stem from
        // the root edge from 1->3 being removed, including the root edge itself

        // When
        adjacencyMaps.add(first);
        adjacencyMaps.add(second);
        adjacencyMaps.add(third);

        // Then
        final AdjacencyMap firstPruned = adjacencyMaps.get(0);
        final AdjacencyMap secondPruned = adjacencyMaps.get(1);
        final AdjacencyMap thirdPruned = adjacencyMaps.get(2);

        assertThat(firstPruned.getDestinations(1), hasSize(1));
        assertThat(secondPruned.getDestinations(2), hasSize(2));
        assertThat(thirdPruned.getDestinations(4), hasSize(2));
        assertThat(thirdPruned.getDestinations(5), hasSize(2));
    }

    private Edge makeEdge(final Object source, final Object destination) {
        return new Edge.Builder().group(TestGroups.EDGE).source(source).dest(destination).directed(true).build();
    }
}
