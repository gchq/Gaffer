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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import uk.gov.gchq.gaffer.data.element.Edge;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;

@RunWith(Parameterized.class)
public class AdjacencyMapsTest {

    private final AdjacencyMaps adjacencyMaps;

    public AdjacencyMapsTest(final AdjacencyMaps adjacencyMaps) {
        this.adjacencyMaps = adjacencyMaps;
    }

    @Parameters
    public static Collection<Object[]> instancesToTest() {
        return Arrays.asList(new Object[][]{
                {new SimpleAdjacencyMaps()},
                {new PrunedAdjacencyMaps()}
        });
    }

    @Before
    public void before() {
        if (null != adjacencyMaps) {
            adjacencyMaps.asList().clear();

            adjacencyMaps.add(getAdjacencyMap(3));
            adjacencyMaps.add(getAdjacencyMap(4));
        }
    }

    @Test
    public void shouldIterate() {
        // Then
        final Iterator<AdjacencyMap> it = adjacencyMaps.iterator();

        final AdjacencyMap first = it.next();
        final AdjacencyMap second = it.next();

        assertThat(first.getAllDestinations(), hasSize(3));
        assertThat(second.getAllDestinations(), hasSize(4));
    }

    @Test
    public void shouldGetSize() {
        // Then
        assertThat(adjacencyMaps.size(), is(2));
    }

    @Test
    public void shouldGetNotEmpty() {
        // Then
        assertThat(adjacencyMaps.empty(), is(false));
    }

    @Test
    public void shouldGetEmpty() {
        // Then
        assertThat(adjacencyMaps.empty(), is(false));
    }

    private AdjacencyMap getAdjacencyMap(final int size) {

        final AdjacencyMap adjacencyMap = new AdjacencyMap();

        for (int i = 0; i < size; i++) {
            adjacencyMap.putEdge(i, i + 1, new Edge(Integer.toString(i), Integer.toString(i), Integer.toString(i + 1), true));
        }

        return adjacencyMap;
    }
}
