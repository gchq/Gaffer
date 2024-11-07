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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class GetElementsHandlerTest {
    private static final int NUM_LOOPS = 10;

    @Test
    void testGetElementsByNonExistentEntityId() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("NOT_PRESENT"))
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEmpty();
    }

    @Test
    void testGetElementsWhenNoIdsAreProvided() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EmptyIterable<>())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEmpty();
    }

    @Test
    void testGetElementsByNonExistentEdgeId() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed("NOT_PRESENT", "ALSO_NOT_PRESENT", true))
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEmpty();
    }

    @Test
    void testGetElementsByEntityId() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEdgeId() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When query for A->B0 with related
        GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .build();
        Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A")
                                || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When query for A->B0 equal
        getElements = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When - query for X-Y0 (undirected) in direction it was inserted in with related
        getElements = new GetElements.Builder()
                .input(new EdgeSeed("X", "Y0", false))
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("X")
                                || ((Entity) element).getVertex().equals("Y0");
                    } else {
                        final Edge edge = (Edge) element;
                        if (edge.isDirected()) {
                            return false;
                        }
                        return (edge.getSource().equals("X") && edge.getDestination().equals("Y0"))
                                || (edge.getSource().equals("Y0") && edge.getDestination().equals("X"));
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When - query for X-Y0 (undirected) in direction it was inserted in with equal
        getElements = new GetElements.Builder()
                .input(new EdgeSeed("X", "Y0", false))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE2)
                        .build())
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When - query for Y0-X (undirected) in opposite direction to which it was inserted in with related
        getElements = new GetElements.Builder()
                .input(new EdgeSeed("Y0", "X", false))
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When - query for Y0-X (undirected) in opposite direction to which it was inserted in with equal
        getElements = new GetElements.Builder()
                .input(new EdgeSeed("Y0", "X", false))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE2)
                        .build())
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testAddAndGetAllElementsNoAggregationAndDuplicateElements() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraphNoAggregation();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getDuplicateElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Map<Element, Integer> resultingElementsToCount = GetAllElementsHandlerTest.streamToCount(
                Streams.toStream(results));
        final Stream<Element> expectedResultsStream = GetAllElementsHandlerTest.getDuplicateElements().stream()
                .filter(element -> element.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A");
                    }
                });
        final Map<Element, Integer> expectedCounts = GetAllElementsHandlerTest.streamToCount(expectedResultsStream);
        assertThat(resultingElementsToCount).isEqualTo(expectedCounts);
    }

    @Test
    void testGetElementsByEntityIdWithViewRestrictedByGroup() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> element.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEdgeIdWithViewRestrictedByGroup() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> element.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A")
                                || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEntityIdWithViewRestrictedByGroupAndAPreAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1)
                        && ((int) e.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEdgeIdWithViewRestrictedByGroupAndAPreAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A")
                                || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1)
                        && ((int) e.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEntityIdWithViewRestrictedByGroupAndAPostAggregationFilter()
            throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1)
                        && ((int) e.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEdgeIdWithViewRestrictedByGroupAndAPostAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A")
                                || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1)
                        && ((int) e.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    private static class ExampleTransform extends KorypheFunction<Integer, Integer> {
        static final int INCREMENT_BY = 100;

        @Override
        public Integer apply(final Integer input) {
            return input + INCREMENT_BY;
        }
    }

    @Test
    void testGetElementsByEntityIdWithViewRestrictedByGroupAndATransform() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new ExampleTransform())
                                        .project(GetAllElementsHandlerTest.COUNT)
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .map(element -> {
                    element.putProperty(GetAllElementsHandlerTest.COUNT,
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT))
                                    + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEdgeIdWithViewRestrictedByGroupAndATransform() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new ExampleTransform())
                                        .project(GetAllElementsHandlerTest.COUNT)
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A")
                                || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .map(element -> {
                    element.putProperty(GetAllElementsHandlerTest.COUNT,
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT))
                                    + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEntityIdWithViewRestrictedByGroupAndAPostTransformFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new ExampleTransform())
                                        .project(GetAllElementsHandlerTest.COUNT)
                                        .build())
                                .postTransformFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(50))
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(e -> ((Edge) e).getSource().equals("A") || ((Edge) e).getDestination().equals("A"))
                .map(element -> {
                    element.putProperty(GetAllElementsHandlerTest.COUNT,
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT))
                                    + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .filter(element -> ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) > 50)
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsByEdgeSeedWithViewRestrictedByGroupAndAPostTransformFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new ExampleTransform())
                                        .project(GetAllElementsHandlerTest.COUNT)
                                        .build())
                                .postTransformFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(50))
                                        .build())
                                .build())
                        .build())
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A")
                                || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .map(element -> {
                    element.putProperty(GetAllElementsHandlerTest.COUNT,
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT))
                                    + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .filter(element -> ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) > 50)
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsIncludeEntitiesOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When view has not entities
        GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .edge(TestGroups.EDGE_2)
                        .build())
                .build();
        Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e instanceof Edge)
                .filter(e -> {
                    final Edge edge = (Edge) e;
                    return edge.getSource().equals("A") || edge.getDestination().equals("A");
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When view has entities
        getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE)
                        .edge(TestGroups.EDGE_2)
                        .build())
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsDirectedTypeOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When directedType is EITHER
        GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .directedType(DirectedType.EITHER)
                .build();
        Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A")
                                || edge.getSource().equals("X") || edge.getDestination().equals("X");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When view has no edges
        getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        return false;
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When directedType is DIRECTED
        getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .directedType(DirectedType.DIRECTED)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        final Edge edge = (Edge) element;
                        return (edge.getSource().equals("A") || edge.getDestination().equals("A")
                                || edge.getSource().equals("X") || edge.getDestination().equals("X"))
                                && edge.isDirected();
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When directedType is UNDIRECTED
        getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .directedType(DirectedType.UNDIRECTED)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        final Edge edge = (Edge) element;
                        return (edge.getSource().equals("A") || edge.getDestination().equals("A")
                                || edge.getSource().equals("X") || edge.getDestination().equals("X"))
                                && !edge.isDirected();
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsInOutTypeOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When inOutType is EITHER
        GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .inOutType(IncludeIncomingOutgoingType.EITHER)
                .build();
        Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A")
                                || edge.getSource().equals("X") || edge.getDestination().equals("X");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When inOutType is INCOMING
        getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .inOutType(IncludeIncomingOutgoingType.INCOMING)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        final Edge edge = (Edge) element;
                        if (edge.isDirected()) {
                            return edge.getDestination().equals("A") || edge.getDestination().equals("X");
                        } else {
                            return edge.getSource().equals("A") || edge.getDestination().equals("A")
                                    || edge.getSource().equals("X") || edge.getDestination().equals("X");
                        }
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When inOutType is OUTGOING
        getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        final Edge edge = (Edge) element;
                        if (edge.isDirected()) {
                            return edge.getSource().equals("A") || edge.getSource().equals("X");
                        } else {
                            return edge.getSource().equals("A") || edge.getDestination().equals("A")
                                    || edge.getSource().equals("X") || edge.getDestination().equals("X");
                        }
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    // Test equivalent for seedMatching
    @Test
    void testGetElementsSeedMatchingTypeOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When equal
        GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .view(new View.Builder()
                        .entity(GetAllElementsHandlerTest.BASIC_ENTITY)
                        .build())
                .build();
        Iterable<? extends Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> element instanceof Entity)
                .filter(element -> {
                    final Entity entity = (Entity) element;
                    return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // When related
        getElements = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("X"))
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A")
                                || edge.getSource().equals("X") || edge.getDestination().equals("X");
                    }
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);

        // Repeat with equal for an EdgeId
        final GetElements getElementsFromEdgeId = new GetElements.Builder()
                .input(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        results = graph.execute(getElementsFromEdgeId, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") && edge.getDestination().equals("B0") && edge.isDirected();
                })
                .forEach(expectedResults::add);
        assertThat(resultsSet).isEqualTo(expectedResults);
    }

    @Test
    void testGetElementsWhenNotMaintainingIndices() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraphNoIndices();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        // Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> graph.execute(getElements, new User()));
    }

    @Test
    void testElementsAreClonedBeforeBeingReturned() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("B9"))
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, new User());
        final Edge result = (Edge) results.iterator().next();

        // Change a property
        result.putProperty(GetAllElementsHandlerTest.PROPERTY1, "qqq");

        // Then
        final Iterable<? extends Element> results2 = graph.execute(getElements, new User());
        final Edge result2 = (Edge) results2.iterator().next();
        assertThat(result2.getDestination()).isEqualTo("B9");
        assertThat(result2.getProperty(GetAllElementsHandlerTest.PROPERTY1)).isEqualTo("q");
    }

    private static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        final Entity entity1 = new Entity(GetAllElementsHandlerTest.BASIC_ENTITY, "A");
        entity1.putProperty(GetAllElementsHandlerTest.PROPERTY1, "p");
        entity1.putProperty(GetAllElementsHandlerTest.COUNT, 1);
        final Entity entity2 = new Entity(GetAllElementsHandlerTest.BASIC_ENTITY, "Z");
        entity2.putProperty(GetAllElementsHandlerTest.PROPERTY1, "p");
        entity2.putProperty(GetAllElementsHandlerTest.COUNT, 1);
        elements.add(entity1);
        IntStream.range(0, NUM_LOOPS)
                .forEach(i -> {
                    elements.add(new Edge.Builder()
                            .group(GetAllElementsHandlerTest.BASIC_EDGE1)
                            .source("A")
                            .dest("B" + i)
                            .directed(true)
                            .property(GetAllElementsHandlerTest.PROPERTY1, "q")
                            .property(GetAllElementsHandlerTest.COUNT, i)
                            .build());

                    elements.add(new Edge.Builder()
                            .group(GetAllElementsHandlerTest.BASIC_EDGE2)
                            .source("X")
                            .dest("Y" + i)
                            .directed(false)
                            .property(GetAllElementsHandlerTest.PROPERTY1, "r")
                            .property(GetAllElementsHandlerTest.PROPERTY2, "s")
                            .property(GetAllElementsHandlerTest.COUNT, 3)
                            .build());
                });
        return elements;
    }

    @Test
    void shouldApplyVisibilityTraitToOperationResults() throws OperationException {
        VisibilityTest.executeOperation(
                new GetElements.Builder()
                        .input(new EntitySeed(VisibilityTest.VERTEX_1), new EntitySeed(VisibilityTest.VERTEX_2))
                        .build(),
                VisibilityTest::elementIterableResultConsumer);
    }
}
