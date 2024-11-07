/*
 * Copyright 2017-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;

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

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static uk.gov.gchq.gaffer.mapstore.impl.VisibilityTest.VERTEX_1;

class GetAdjacentIdsTest {

    @Test
    void shouldGetAdjacentIdsWhenThereAreNone() throws OperationException {
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
        final Iterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

        // Then
        final Set<EntityId> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEmpty();
    }

    @Test
    void shouldGetAdjacentEntityId() throws OperationException {
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
        Iterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

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
        assertThat(resultsSet).isEqualTo(expectedResults);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

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
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void shouldGetAdjacentEntityIdWithViewRestrictedByGroup() throws OperationException {
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
        final Iterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

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
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void shouldGetElementsByEntityIdWithViewRestrictedByGroupAndAPreAggregationFilter()
            throws OperationException {
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
        final Iterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

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
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void shouldGetElementsByEntityIdWithViewRestrictedByGroupAndAPostAggregationFilter()
            throws OperationException {
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
        final Iterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

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
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void shouldFailValidationWhenEntityHasFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new GetAdjacentIds.Builder()
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
                        .build())
                .withMessageContaining("View should not have entities with filters.");
    }

    @Test
    void shouldPassValidationOnEntitiesWithoutFilters() throws OperationException {
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

        final Iterable<? extends EntityId> results = graph.execute(getAdjacentIds, new User());

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
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void shouldApplyVisibilityTraitToOperationResults() throws OperationException {
        VisibilityTest.executeOperation(new GetAdjacentIds.Builder().input(new EntitySeed(VERTEX_1)).build(),
                VisibilityTest::vertex1AdjacentIdsResultConsumer);
    }
}
