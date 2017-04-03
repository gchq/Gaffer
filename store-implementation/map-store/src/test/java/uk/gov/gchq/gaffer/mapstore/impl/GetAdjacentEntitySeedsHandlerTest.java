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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.impl.GetAllElementsHandlerTest;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

public class GetAdjacentEntitySeedsHandlerTest {

    @Test
    public void testGetAdjacentEntitySeedsWhenThereAreNone() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed("NOT_PRESENT"))
                .build();
        final CloseableIterable<EntitySeed> results = graph.execute(getAdjacentEntitySeeds, new User());

        // Then
        final Set<EntitySeed> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(Collections.emptySet(), resultsSet);
    }

    @Test
    public void testGetAdjacentEntitySeed() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When - query for A
        GetAdjacentEntitySeeds getAdjacentEntitySeeds = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed("A"))
                .build();
        CloseableIterable<EntitySeed> results = graph.execute(getAdjacentEntitySeeds, new User());

        // Then
        final Set<EntitySeed> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<EntitySeed> expectedResults = new HashSet<>();
        GetAllElementsHandlerTest.getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    if (edge.getSource().equals("A") || edge.getDestination().equals("A")) {
                        return true;
                    }
                    return false;
                })
                .map(element -> {
                    final Edge edge = (Edge) element;
                    final Set<EntitySeed> nodes = new HashSet<>();
                    nodes.add(new EntitySeed(edge.getSource()));
                    nodes.add(new EntitySeed(edge.getDestination()));
                    return nodes;
                })
                .flatMap(nodes -> nodes.stream())
                .forEach(expectedResults::add);
        expectedResults.remove(new EntitySeed("A"));
        assertEquals(expectedResults, resultsSet);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(expectedResults, resultsSet);

        // When - query for A and Y2
        getAdjacentEntitySeeds = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("Y2"))
                .build();
        results = graph.execute(getAdjacentEntitySeeds, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        GetAllElementsHandlerTest.getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") || edge.getDestination().equals("A")
                            || edge.getSource().equals("Y2") || edge.getDestination().equals("Y2");
                })
                .map(element -> {
                    final Edge edge = (Edge) element;
                    final Set<EntitySeed> nodes = new HashSet<>();
                    nodes.add(new EntitySeed(edge.getSource()));
                    nodes.add(new EntitySeed(edge.getDestination()));
                    return nodes;
                })
                .flatMap(nodes -> nodes.stream())
                .forEach(expectedResults::add);
        expectedResults.remove(new EntitySeed("A"));
        expectedResults.remove(new EntitySeed("Y2"));
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAdjacentEntitySeedWithViewRestrictedByGroup() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("Y2"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE2)
                        .build())
                .build();
        final CloseableIterable<EntitySeed> results = graph.execute(getAdjacentEntitySeeds, new User());

        // Then
        final Set<EntitySeed> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<EntitySeed> expectedResults = new HashSet<>();
        GetAllElementsHandlerTest.getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> element.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE2))
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") || edge.getDestination().equals("A")
                            || edge.getSource().equals("Y2") || edge.getDestination().equals("Y2");
                })
                .map(element -> {
                    final Edge edge = (Edge) element;
                    final Set<EntitySeed> nodes = new HashSet<>();
                    nodes.add(new EntitySeed(edge.getSource()));
                    nodes.add(new EntitySeed(edge.getDestination()));
                    return nodes;
                })
                .flatMap(nodes -> nodes.stream())
                .forEach(expectedResults::add);
        expectedResults.remove(new EntitySeed("A"));
        expectedResults.remove(new EntitySeed("Y2"));
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEntitySeedWithViewRestrictedByGroupAndAPreAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("Y2"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<EntitySeed> results = graph.execute(getAdjacentEntitySeeds, new User());

        // Then
        final Set<EntitySeed> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<EntitySeed> expectedResults = new HashSet<>();
        GetAllElementsHandlerTest.getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> element.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") || edge.getDestination().equals("A")
                            || edge.getSource().equals("Y2") || edge.getDestination().equals("Y2");
                })
                .filter(element -> ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .map(element -> {
                    final Edge edge = (Edge) element;
                    final Set<EntitySeed> nodes = new HashSet<>();
                    nodes.add(new EntitySeed(edge.getSource()));
                    nodes.add(new EntitySeed(edge.getDestination()));
                    return nodes;
                })
                .flatMap(nodes -> nodes.stream())
                .forEach(expectedResults::add);
        expectedResults.remove(new EntitySeed("A"));
        expectedResults.remove(new EntitySeed("Y2"));
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEntitySeedWithViewRestrictedByGroupAndAPostAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("Y2"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<EntitySeed> results = graph.execute(getAdjacentEntitySeeds, new User());

        // Then
        final Set<EntitySeed> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<EntitySeed> expectedResults = new HashSet<>();
        GetAllElementsHandlerTest.getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> element.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") || edge.getDestination().equals("A")
                            || edge.getSource().equals("Y2") || edge.getDestination().equals("Y2");
                })
                .filter(element -> ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .map(element -> {
                    final Edge edge = (Edge) element;
                    final Set<EntitySeed> nodes = new HashSet<>();
                    nodes.add(new EntitySeed(edge.getSource()));
                    nodes.add(new EntitySeed(edge.getDestination()));
                    return nodes;
                })
                .flatMap(nodes -> nodes.stream())
                .forEach(expectedResults::add);
        expectedResults.remove(new EntitySeed("A"));
        expectedResults.remove(new EntitySeed("Y2"));
        assertEquals(expectedResults, resultsSet);
    }
}
