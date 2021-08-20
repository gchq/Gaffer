/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import uk.gov.gchq.gaffer.data.element.Edge;

import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class AdjacencyMapsTest {

    private static Stream<Arguments> instancesToTest() {
        return Stream.of(
                Arguments.of(testSetUp(new SimpleAdjacencyMaps())),
                Arguments.of(testSetUp(new PrunedAdjacencyMaps()))
        );
    }

    private static AdjacencyMaps testSetUp(final AdjacencyMaps adjacencyMaps) {
        adjacencyMaps.add(getAdjacencyMap(3));
        adjacencyMaps.add(getAdjacencyMap(4));
        return adjacencyMaps;
    }

    @ParameterizedTest
    @MethodSource("instancesToTest")
    public void shouldIterate(AdjacencyMaps adjacencyMaps) {
        final Iterator<AdjacencyMap> it = adjacencyMaps.iterator();

        final AdjacencyMap first = it.next();
        final AdjacencyMap second = it.next();

        assertThat(first.getAllDestinations()).hasSize(3);
        assertThat(second.getAllDestinations()).hasSize(4);
    }

    @ParameterizedTest
    @MethodSource("instancesToTest")
    public void shouldGetSize(AdjacencyMaps adjacencyMaps) {
        assertThat(adjacencyMaps).hasSize(2);
    }

    @ParameterizedTest
    @MethodSource("instancesToTest")
    public void shouldGetNotEmpty(AdjacencyMaps adjacencyMaps) {
        assertThat(adjacencyMaps.empty()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("instancesToTest")
    public void shouldGetEmpty(AdjacencyMaps adjacencyMaps) {
        assertThat(adjacencyMaps.empty()).isFalse();
    }

    private static AdjacencyMap getAdjacencyMap(final int size) {
        final AdjacencyMap adjacencyMap = new AdjacencyMap();
        for (int i = 0; i < size; i++) {
            adjacencyMap.putEdge(i, i + 1, new Edge(Integer.toString(i), Integer.toString(i), Integer.toString(i + 1), true));
        }
        return adjacencyMap;
    }
}
