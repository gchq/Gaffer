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
package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.FlatMap;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToVerticesHandler;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.function.FirstItem;
import uk.gov.gchq.koryphe.impl.function.NthItem;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class FlatMapHandlerTest {
    private FlatMapHandler<Integer, Integer> handler;
    private Context context;
    private Store store;
    private Function<Iterable<Integer>, Integer> function;
    private Iterable<Iterable<Integer>> input;
    private FlatMap<Integer, Integer> operation;

    private final Edge EDGE_AB = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("B").directed(true).build();
    private final Edge EDGE_BC = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("C").directed(true).build();
    private final Edge EDGE_BD = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("D").directed(true).build();
    private final Edge EDGE_CA = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("A").directed(true).build();
    private final Edge EDGE_CB = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("B").directed(true).build();
    private final Edge EDGE_DA = new Edge.Builder().group(TestGroups.EDGE).source("D").dest("A").directed(true).build();

    private final static Entity ENTITY_A = new Entity.Builder().group(TestGroups.ENTITY).vertex("A").build();
    private final static Entity ENTITY_B = new Entity.Builder().group(TestGroups.ENTITY).vertex("B").build();
    private final static Entity ENTITY_C = new Entity.Builder().group(TestGroups.ENTITY).vertex("C").build();
    private final static Entity ENTITY_D = new Entity.Builder().group(TestGroups.ENTITY).vertex("D").build();

    private final Walk walk = new Walk.Builder()
            .edge(EDGE_AB)
            .entity(ENTITY_B)
            .edge(EDGE_BC)
            .entity(ENTITY_C)
            .edge(EDGE_CA)
            .build();

    private final Walk walk1 = new Walk.Builder()
            .edge(EDGE_CB)
            .entities(ENTITY_B)
            .edge(EDGE_BD)
            .entities(ENTITY_D)
            .edge(EDGE_DA)
            .build();

    @Before
    public void setup() {
        handler = new FlatMapHandler<>();
        context = mock(Context.class);
        store = mock(Store.class);
        function = mock(Function.class);
        final List<Integer> first = Arrays.asList(1, 2, 3);
        final List<Integer> second = Arrays.asList(4, 5, 6);
        final List<Integer> third = Arrays.asList(7, 8, 9);
        input = Arrays.asList(first, second, third);

        given(context.getUser()).willReturn(new User());
        given(store.getProperties()).willReturn(new StoreProperties());
    }

    @Test
    public void shouldHandleNullOperation() {
        // Given
        operation = null;

        // When / Then
        try {
            handler.doOperation(operation, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Operation cannot be null"));
        }
    }

    @Test
    public void shouldHandleNullInput() {
        // Given
        operation = new FlatMap.Builder<Integer, Integer>()
                .input(null)
                .function(function)
                .build();

        // When / Then
        try {
            handler.doOperation(operation, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Input cannot be null"));
        }
    }

    @Test
    public void shouldHandleNullFunction() {
        // Given
        operation = new FlatMap.Builder<Integer, Integer>()
                .input(input)
                .function(null)
                .build();

        // When / Then
        try {
            handler.doOperation(operation, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Function cannot be null"));
        }
    }

    @Test
    public void shouldReturnIterableFromOperation() throws OperationException {
        // Given
        operation = new FlatMap.Builder<Integer, Integer>()
                .input(input)
                .function(new NthItem<>(1))
                .build();

        // When
        final Iterable<Integer> result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        assertEquals(Arrays.asList(2, 5, 8), result);
    }

    @Test
    public void shouldProcessWalkObjects() throws OperationException {
        // Given
        final FlatMap<Set<Edge>, Set<Edge>> flatMap = new FlatMap.Builder<Set<Edge>, Set<Edge>>()
                .input(Arrays.asList(walk, walk1))
                .function(new FirstItem<>())
                .build();

        final FlatMapHandler<Set<Edge>, Set<Edge>> testHandler = new FlatMapHandler<>();

        final Iterable<Iterable<Edge>> expectedResults = Arrays.asList(
                Sets.newHashSet(EDGE_AB),
                Sets.newHashSet(EDGE_CB));

        // When
        final Iterable<Set<Edge>> results = testHandler.doOperation(flatMap, context, store);

        // Then
        assertEquals(expectedResults, results);

        // Given 2
        final FlatMap<Edge, Edge> flatMap1 = new FlatMap.Builder<Edge, Edge>()
                .input(expectedResults)
                .function(new FirstItem<>())
                .build();

        final FlatMapHandler<Edge, Edge> testHandler1 = new FlatMapHandler<>();

        final Iterable<Edge> expectedResults1 = Arrays.asList(EDGE_AB, EDGE_CB);

        // When 2
        final Iterable<Edge> results1 = testHandler1.doOperation(flatMap1, context, store);

        // Then 2
        assertEquals(expectedResults1, results1);

        // Given / When 3
        final Iterable<? extends Object> vertices = new ToVerticesHandler()
                .doOperation(new ToVertices.Builder()
                        .input(results1)
                        .edgeVertices(ToVertices.EdgeVertices.SOURCE)
                        .build(), context, store);

        final Iterable<? extends Object> deduplicated = new ToSetHandler<>()
                .doOperation(new ToSet.Builder<>()
                        .input(vertices)
                        .build(), context, store);

        // Then
        assertThat(deduplicated, containsInAnyOrder("A", "C"));
    }

    @Test
    public void shouldProcessWalkInOperationChainCorrectly() throws OperationException {
        // Given
        final FlatMap<Set<Edge>, Set<Edge>> firstFlatMap = new FlatMap.Builder<Set<Edge>, Set<Edge>>()
                .input(Arrays.asList(walk, walk1))
                .function(new FirstItem<>())
                .build();

        final FlatMap<Edge, Edge> secondFlatMap = new FlatMap.Builder<Edge, Edge>()
                .function(new FirstItem<>())
                .build();

        final ToVertices toVertices = new ToVertices.Builder()
                .edgeVertices(ToVertices.EdgeVertices.SOURCE)
                .build();

        final ToSet toSet = new ToSet();

        final OperationChain<Set<?>> opChain = new OperationChain<>(
                Arrays.asList(
                        firstFlatMap,
                        secondFlatMap,
                        toVertices,
                        toSet));

//                new OperationChain.Builder()
//                .first(firstFlatMap)
//                .then(secondFlatMap)
//                .then(new ToVertices())
//                .then(new ToSet<>())
//                .build();

        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();
        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        final OperationChainHandler<Set<?>> opChainHandler = new OperationChainHandler<>(opChainValidator, opChainOptimisers);

        given(store.handleOperation(firstFlatMap, context)).willReturn(Arrays.asList(
                Sets.newHashSet(EDGE_AB),
                Sets.newHashSet(EDGE_CB)));
        given(store.handleOperation(secondFlatMap, context)).willReturn(Arrays.asList(EDGE_AB, EDGE_CB));
        given(store.handleOperation(toVertices, context)).willReturn(Arrays.asList("A", "C"));
        given(store.handleOperation(toSet, context)).willReturn(Sets.newHashSet("A", "C"));

        // When
        final Iterable<?> results = opChainHandler.doOperation(opChain, context, store);

        // Then
        assertThat(results, containsInAnyOrder("A", "C"));
    }
}
