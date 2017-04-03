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
package uk.gov.gchq.gaffer.mapstore.impl;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

public class OperationChainTest {

    @Test
    public void testOperationChain() throws StoreException, OperationException {
        // Given
        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.openStreams(getClass(), "example-schema"))
                .storeProperties(new MapStoreProperties())
                .build();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final CloseableIterable<Edge> results = graph.execute(new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds.Builder()
                        .addSeed(new EntitySeed("vertex1"))
                        .build())
                .then(new GetEdges<>())
                .build(), new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e instanceof Edge)
                .filter(e -> {
                    final Edge edge = (Edge) e;
                    return edge.getSource().equals("vertex1") || edge.getDestination().equals("vertex2");
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    private static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        final Entity entity1 = new Entity("entity", "vertex1");
        entity1.putProperty("count", 1);
        elements.add(entity1);
        final Entity entity2 = new Entity("entity", "vertex2");
        entity2.putProperty("count", 2);
        elements.add(entity2);
        final Edge edge = new Edge("edge", "vertex1", "vertex2", true);
        edge.putProperty("count", 1);
        elements.add(edge);
        return elements;
    }
}
