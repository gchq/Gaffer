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
package uk.gov.gchq.gaffer.mapstore.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetAdjacentIdsTest {

    @Test
    public void shouldGetAdjacentIdsWhenThereAreNone() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .input(new EntitySeed("NOT_PRESENT"))
                .build();
        final CloseableIterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

        // Then
        final Set<EntityId> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        assertEquals(Collections.emptySet(), resultsSet);
    }

    @Test
    public void shouldGetAdjacentEntityId() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When - query for A
        GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"))
                .build();
        CloseableIterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

        // Then
        final Set<EntityId> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<EntityId> expectedResults = new HashSet<>();
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
                    final Set<EntityId> nodes = new HashSet<>();
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
        Streams.toStream(results).forEach(resultsSet::add);
        assertEquals(expectedResults, resultsSet);

        // When - query for A and Y2
        getAdjacentIds = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"), new EntitySeed("Y2"))
                .build();
        results = graph.execute(getAdjacentIds, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
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
                    final Set<EntityId> nodes = new HashSet<>();
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
    public void shouldGetAdjacentEntityIdWithViewRestrictedByGroup() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"), new EntitySeed("Y2"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE2)
                        .build())
                .build();
        final CloseableIterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

        // Then
        final Set<EntityId> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<EntityId> expectedResults = new HashSet<>();
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
                    final Set<EntityId> nodes = new HashSet<>();
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
    public void shouldGetElementsByEntityIdWithViewRestrictedByGroupAndAPreAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"), new EntitySeed("Y2"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

        // Then
        final Set<EntityId> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<EntityId> expectedResults = new HashSet<>();
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
                    final Set<EntityId> nodes = new HashSet<>();
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
    public void shouldGetElementsByEntityIdWithViewRestrictedByGroupAndAPostAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"), new EntitySeed("Y2"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

        // Then
        final Set<EntityId> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<EntityId> expectedResults = new HashSet<>();
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
                    final Set<EntityId> nodes = new HashSet<>();
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
    public void shouldFailValidationWhenEntityHasFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When / Then
        try {
            final GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                    .input(new EntitySeed("A"), new EntitySeed("Y2"))
                    .view(new View.Builder()
                                  .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                          .postAggregationFilter(new ElementFilter.Builder()
                                                                         .select(GetAllElementsHandlerTest.COUNT)
                                                                         .execute(new IsMoreThan(5))
                                                                         .build())
                                          .build())
                          .entity(GetAllElementsHandlerTest.BASIC_ENTITY, new ViewElementDefinition.Builder()
                                  .postAggregationFilter(new ElementFilter.Builder()
                                                        .select(GetAllElementsHandlerTest.PROPERTY1)
                                                        .execute(new IsEqual("string"))
                                                        .build())
                                  .build())
                          .build())
                    .build();
            final CloseableIterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("View should not have entities with filters."));
        }
    }

    @Test
    public void shouldPassValidationOnEntitiesWithoutFilters() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"), new EntitySeed("Y2"))
                .view(new View.Builder()
                      .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                            .postAggregationFilter(new ElementFilter.Builder()
                                                   .select(GetAllElementsHandlerTest.COUNT)
                                                   .execute(new IsMoreThan(5))
                                                   .build())
                            .build())
                      .entity(GetAllElementsHandlerTest.BASIC_ENTITY, new ViewElementDefinition())
                      .build())
                .build();

        final CloseableIterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

        // Then
        final Set<EntityId> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<EntityId> expectedResults = new HashSet<>();
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
                    final Set<EntityId> nodes = new HashSet<>();
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
