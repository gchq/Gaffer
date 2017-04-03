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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.SimpleTransformFunction;
import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

public class GetElementsHandlerTest {
    private final static int NUM_LOOPS = 10;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testGetElementsByNonExistentEntitySeed() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("NOT_PRESENT"))
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(Collections.emptySet(), resultsSet);
    }

    @Test
    public void testGetElementsWhenNoEntitySeedsProvided() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(Collections.emptySet(), resultsSet);
    }

    @Test
    public void testGetElementsByNonExistentEdgeSeed() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("NOT_PRESENT", "ALSO_NOT_PRESENT", true))
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(Collections.emptySet(), resultsSet);
    }

    @Test
    public void testGetElementsWhenNoEdgeSeedsProvided() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(Collections.emptySet(), resultsSet);
    }

    @Test
    public void testGetElementsByEntitySeed() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEdgeSeed() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When query for A->B0 with seedMatching set to RELATED
        GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
                .seedMatching(GetOperation.SeedMatchingType.RELATED)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A") || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(expectedResults, resultsSet);

        // When query for A->B0 with seedMatching set to EQUAL
        getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
                .seedMatching(GetOperation.SeedMatchingType.EQUAL)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When - query for X-Y0 (undirected) in direction it was inserted in with seedMatching set to RELATED
        getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("X", "Y0", false))
                .seedMatching(GetOperation.SeedMatchingType.RELATED)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("X") || ((Entity) element).getVertex().equals("Y0");
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
        assertEquals(expectedResults, resultsSet);

        // When - query for X-Y0 (undirected) in direction it was inserted in with seedMatching set to EQUAL
        getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("X", "Y0", false))
                .seedMatching(GetOperation.SeedMatchingType.EQUAL)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(expectedResults, resultsSet);

        // When - query for Y0-X (undirected) in opposite direction to which it was inserted in with seedMatching set to
        // RELATED
        getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("Y0", "X", false))
                .seedMatching(GetOperation.SeedMatchingType.RELATED)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(expectedResults, resultsSet);

        // When - query for Y0-X (undirected) in opposite direction to which it was inserted in with seedMatching set to
        // EQUAL
        getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("Y0", "X", false))
                .seedMatching(GetOperation.SeedMatchingType.EQUAL)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testAddAndGetAllElementsNoAggregationAndDuplicateElements() throws StoreException, OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraphNoAggregation();
        final AddElements addElements = new AddElements.Builder()
                .elements(GetAllElementsHandlerTest.getDuplicateElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Map<Element, Integer> resultingElementsToCount = GetAllElementsHandlerTest.streamToCount(
                StreamSupport.stream(results.spliterator(), false));
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
        assertEquals(expectedCounts, resultingElementsToCount);
    }

    @Test
    public void testGetElementsByEntitySeedWithViewRestrictedByGroup() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEdgeSeedWithViewRestrictedByGroup() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1)
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> element.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A") || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEntitySeedWithViewRestrictedByGroupAndAPreAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEdgeSeedWithViewRestrictedByGroupAndAPreAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A") || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1)
                        && ((int) e.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEntitySeedWithViewRestrictedByGroupAndAPostAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEdgeSeedWithViewRestrictedByGroupAndAPostAggregationFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
                .view(new View.Builder()
                        .edge(GetAllElementsHandlerTest.BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(GetAllElementsHandlerTest.COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A") || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1)
                        && ((int) e.getProperty(GetAllElementsHandlerTest.COUNT)) > 5)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Inputs(Integer.class)
    @Outputs(Integer.class)
    private static class ExampleTransform extends SimpleTransformFunction<Integer> {
        static final int INCREMENT_BY = 100;

        @Override
        protected Integer _transform(final Integer input) {
            return input + INCREMENT_BY;
        }

        @Override
        public TransformFunction statelessClone() {
            return new ExampleTransform();
        }
    }

    @Test
    public void testGetElementsByEntitySeedWithViewRestrictedByGroupAndATransform() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
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
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEdgeSeedWithViewRestrictedByGroupAndATransform() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
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
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A") || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .map(element -> {
                    element.putProperty(GetAllElementsHandlerTest.COUNT,
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEntitySeedWithViewRestrictedByGroupAndAPostTransformFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
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
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(e -> ((Edge) e).getSource().equals("A") || ((Edge) e).getDestination().equals("A"))
                .map(element -> {
                    element.putProperty(GetAllElementsHandlerTest.COUNT,
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .filter(element -> ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) > 50)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsByEdgeSeedWithViewRestrictedByGroupAndAPostTransformFilter() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EdgeSeed, Element> getElements = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
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
        final CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(GetAllElementsHandlerTest.BASIC_EDGE1))
                .filter(element -> {
                    if (element instanceof Entity) {
                        return ((Entity) element).getVertex().equals("A") || ((Entity) element).getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") && edge.getDestination().equals("B0");
                    }
                })
                .map(element -> {
                    element.putProperty(GetAllElementsHandlerTest.COUNT,
                            ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) + ExampleTransform.INCREMENT_BY);
                    return element;
                })
                .filter(element -> ((Integer) element.getProperty(GetAllElementsHandlerTest.COUNT)) > 50)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsIncludeEntitiesOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When includeEntities is false
        GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .includeEntities(false)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e instanceof Edge)
                .filter(e -> {
                    final Edge edge = (Edge) e;
                    return edge.getSource().equals("A") || edge.getDestination().equals("A");
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When includeEntities is true
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .includeEntities(true)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsIncludeEdgesOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When includeEdges is ALL
        GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .includeEdges(GetOperation.IncludeEdgeType.ALL)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // When includeEdges is NONE
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .includeEdges(GetOperation.IncludeEdgeType.NONE)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // When includeEdges is DIRECTED
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .includeEdges(GetOperation.IncludeEdgeType.DIRECTED)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // When includeEdges is UNDIRECTED
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsInOutTypeOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When inOutType is BOTH
        GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .inOutType(GetOperation.IncludeIncomingOutgoingType.BOTH)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // When inOutType is INCOMING
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .inOutType(GetOperation.IncludeIncomingOutgoingType.INCOMING)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // When inOutType is OUTGOING
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .inOutType(GetOperation.IncludeIncomingOutgoingType.OUTGOING)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsSeedMatchingTypeOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When seedMatching is EQUAL
        GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .seedMatching(GetOperation.SeedMatchingType.EQUAL)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> element instanceof Entity)
                .filter(element -> {
                    final Entity entity = (Entity) element;
                    return entity.getVertex().equals("A") || entity.getVertex().equals("X");
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When seedMatching is RELATED
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .seedMatching(GetOperation.SeedMatchingType.RELATED)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // Repeat with seedMatching set to EQUAL for an EdgeSeed
        final GetElements<EdgeSeed, Element> getElementsFromEdgeSeed = new GetElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed("A", "B0", true))
                .seedMatching(GetOperation.SeedMatchingType.EQUAL)
                .build();
        results = graph.execute(getElementsFromEdgeSeed, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Edge)
                .filter(element -> {
                    final Edge edge = (Edge) element;
                    return edge.getSource().equals("A") && edge.getDestination().equals("B0") && edge.isDirected();
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsPopulatePropertiesOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When populateProperties is true
        GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .populateProperties(true)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
        assertEquals(expectedResults, resultsSet);

        // When populateProperties is false
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .populateProperties(false)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
                .map(element -> element.emptyClone())
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsLimitResultsOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When limitResults is 1
        GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .limitResults(1)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(1, resultsSet.size());

        // When limitResults is 2
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("X"))
                .limitResults(2)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(2, resultsSet.size());
    }

    @Test
    public void testGetElementsDeduplicateOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When deduplicate is false
        // Use A and B0 as seeds so that get the same edge back twice
        GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("B0"))
                .deduplicate(false)
                .build();
        CloseableIterable<Element> results = graph.execute(getElements, new User());

        // Then
        assertEquals(NUM_LOOPS + 2L, StreamSupport.stream(results.spliterator(), false).count());

        // When deduplicate is true
        getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("B0"))
                .deduplicate(true)
                .build();
        results = graph.execute(getElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(element -> {
                    if (element instanceof Entity) {
                        final Entity entity = (Entity) element;
                        return entity.getVertex().equals("A") || entity.getVertex().equals("B0");
                    } else {
                        final Edge edge = (Edge) element;
                        return edge.getSource().equals("A") || edge.getDestination().equals("A")
                                || edge.getSource().equals("B0") || edge.getDestination().equals("B0");
                    }
                })
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetElementsWhenNotMaintainingIndices() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraphNoIndices();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .deduplicate(false)
                .build();

        // Then
        exception.expect(OperationException.class);
        final CloseableIterable<Element> results = graph.execute(getElements, new User());
    }

    @Test
    public void testElementsAreClonedBeforeBeingReturned() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetElements<EntitySeed, Element> getElements = new GetElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("B9"))
                .deduplicate(false)
                .build();
        final Edge result = (Edge) graph.execute(getElements, new User()).iterator().next();
        // Change a property
        result.setDestination("BBB");
        result.putProperty(GetAllElementsHandlerTest.PROPERTY1, "qqq");

        // Then
        final Edge result2 = (Edge) graph.execute(getElements, new User()).iterator().next();
        assertEquals("B9", result2.getDestination());
        assertEquals("q", result2.getProperty(GetAllElementsHandlerTest.PROPERTY1));
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
                    final Edge edge1 = new Edge(GetAllElementsHandlerTest.BASIC_EDGE1, "A", "B" + i, true);
                    edge1.putProperty(GetAllElementsHandlerTest.PROPERTY1, "q");
                    edge1.putProperty(GetAllElementsHandlerTest.COUNT, i);
                    elements.add(edge1);
                    final Edge edge2 = new Edge(GetAllElementsHandlerTest.BASIC_EDGE2, "X", "Y" + i, false);
                    edge2.putProperty(GetAllElementsHandlerTest.PROPERTY1, "r");
                    edge2.putProperty(GetAllElementsHandlerTest.PROPERTY2, "s");
                    edge2.putProperty(GetAllElementsHandlerTest.COUNT, 3);
                    elements.add(edge2);
                });
        return elements;
    }
}
