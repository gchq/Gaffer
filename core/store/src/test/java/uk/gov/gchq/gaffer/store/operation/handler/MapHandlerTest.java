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
package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.data.graph.function.walk.ExtractWalkEdgesFromHop;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.function.FirstItem;
import uk.gov.gchq.koryphe.impl.function.IterableConcat;
import uk.gov.gchq.koryphe.impl.function.IterableFunction;
import uk.gov.gchq.koryphe.impl.function.NthItem;
import uk.gov.gchq.koryphe.impl.function.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class MapHandlerTest {
    private Context context;
    private Store store;
    private Function<Integer, Integer> function;
    private Integer input;

    private static final Edge EDGE_AB = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("B").directed(true).build();
    private static final Edge EDGE_BC = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("C").directed(true).build();
    private static final Edge EDGE_BD = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("D").directed(true).build();
    private static final Edge EDGE_CA = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("A").directed(true).build();
    private static final Edge EDGE_CB = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("B").directed(true).build();
    private static final Edge EDGE_DA = new Edge.Builder().group(TestGroups.EDGE).source("D").dest("A").directed(true).build();

    private static final Entity ENTITY_B = new Entity.Builder().group(TestGroups.ENTITY).vertex("B").build();
    private static final Entity ENTITY_C = new Entity.Builder().group(TestGroups.ENTITY).vertex("C").build();
    private static final Entity ENTITY_D = new Entity.Builder().group(TestGroups.ENTITY).vertex("D").build();

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
        context = mock(Context.class);
        store = mock(Store.class);
        function = mock(Function.class);
        input = 3;

        given(context.getUser()).willReturn(new User());
        given(store.getProperties()).willReturn(new StoreProperties());
        given(function.apply(input)).willReturn(6);
    }

    @Test
    public void shouldHandleNullOperation() {
        // Given
        final MapHandler<Integer, Integer> handler = new MapHandler<>();

        final Map<Integer, Integer> operation = null;

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
        final MapHandler<Integer, Integer> handler = new MapHandler<>();

        final Map<Integer, Integer> operation = new Map.Builder<Integer>()
                .input(null)
                .first(function)
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
        final MapHandler<Integer, Integer> handler = new MapHandler<>();

        function = null;

        final Map<Integer, Integer> operation = new Map.Builder<Integer>()
                .input(input)
                .first(function)
                .build();

        // When / Then
        try {
            handler.doOperation(operation, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Function cannot be null"));
        }
    }

    @Test
    public void shouldReturnItemFromOperationWithMockFunction() throws OperationException {
        // Given
        final MapHandler<Integer, Integer> handler = new MapHandler<>();

        final Map<Integer, Integer> operation = new Map.Builder<Integer>()
                .input(input)
                .first(function)
                .build();

        // When
        final Integer result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        assertEquals(new Integer(6), result);
    }

    @Test
    public void shouldMapSingleObject() throws OperationException {
        // Given
        final MapHandler<Integer, String> handler = new MapHandler<>();

        final Map<Integer, String> operation = new Map.Builder<Integer>()
                .input(7)
                .first(Object::toString)
                .build();

        // When
        final String result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        assertEquals("7", result);
    }

    @Test
    public void shouldMapMultipleObjectsAtOnce() throws OperationException {
        // Given
        final MapHandler<Iterable<Integer>, String> handler = new MapHandler<>();

        final Map<Iterable<Integer>, String> operation = new Map.Builder<Iterable<Integer>>()
                .input(Arrays.asList(1, 2))
                .first(Object::toString)
                .build();

        // When
        final String result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        assertEquals("[1, 2]", result);
    }

    @Test
    public void shouldMapMultipleObjects() throws OperationException {
        // Given
        final MapHandler<Iterable<Integer>, Iterable<String>> handler = new MapHandler<>();

        final Map<Iterable<Integer>, Iterable<String>> operation = new Map.Builder<Iterable<Integer>>()
                .input(Arrays.asList(1, 2))
                .first(new IterableFunction<Integer, String>(Object::toString))
                .build();

        // When
        final Iterable<String> result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        assertEquals(Arrays.asList("1", "2"), Lists.newArrayList(result));
    }

    @Test
    public void shouldExtractFirstItem() throws OperationException {
        // Given
        final MapHandler<Iterable<Iterable<Integer>>, Iterable<Integer>> handler = new MapHandler<>();

        final Map<Iterable<Iterable<Integer>>, Iterable<Integer>> operation = new Map.Builder<Iterable<Iterable<Integer>>>()
                .input(Arrays.asList(
                        Arrays.asList(1, 2),
                        Arrays.asList(3, 4)))
                .first(new FirstItem<>())
                .build();

        // When
        final Iterable<Integer> result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        assertEquals(Arrays.asList(1, 2), Lists.newArrayList(result));
    }

    @Test
    public void shouldFlatMapMultipleObjects() throws OperationException {
        // Given
        final MapHandler<Iterable<Iterable<Integer>>, Iterable<Integer>> handler = new MapHandler<>();

        final Map<Iterable<Iterable<Integer>>, Iterable<Integer>> operation = new Map.Builder<Iterable<Iterable<Integer>>>()
                .input(Arrays.asList(
                        Arrays.asList(1, 2),
                        Arrays.asList(3, 4)))
                .first(new IterableConcat<>())
                .build();

        // When
        final Iterable<Integer> result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        assertEquals(Arrays.asList(1, 2, 3, 4), Lists.newArrayList(result));
    }

    @Test
    public void shouldReturnIterableFromOperation() throws OperationException {
        // Given
        final Iterable<Iterable<Integer>> input = Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(4, 5, 6),
                Arrays.asList(7, 8, 9));

        final MapHandler<Iterable<Iterable<Integer>>, String> handler = new MapHandler<>();

        final Map<Iterable<Iterable<Integer>>, String> operation = new Map.Builder<Iterable<Iterable<Integer>>>()
                .input(input)
                .first(new IterableFunction.Builder<Iterable<Integer>>()
                        .first(new NthItem<>(1))
                        .then(Object::toString)
                        .build())
                .then(new NthItem<>(2))
                .build();

        // When
        final String results = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(results);
        assertEquals("8", results);
    }

    @Test
    public void shouldProcessWalkObjects() throws OperationException {
        // Given
        final Iterable<Iterable<Set<Edge>>> walks = Arrays.asList(walk, walk1);

        final Map<Iterable<Iterable<Set<Edge>>>, Iterable<Set<Edge>>> map = new Map.Builder<Iterable<Iterable<Set<Edge>>>>()
                .input(walks)
                .first(new IterableFunction.Builder<Iterable<Set<Edge>>>()
                        .first(new FirstItem<>())
                        .build())
                .build();

        final MapHandler<Iterable<Iterable<Set<Edge>>>, Iterable<Set<Edge>>> handler = new MapHandler<>();

        // When
        final Iterable<Set<Edge>> results = handler.doOperation(map, context, store);

        final Iterable<Iterable<Edge>> expectedResults = Arrays.asList(
                Sets.newHashSet(EDGE_AB),
                Sets.newHashSet(EDGE_CB));

        // Then
        assertNotNull(results);
        assertEquals(expectedResults, Lists.newArrayList(results));
    }

    @Test
    public void shouldProcessWalksInOperationChain() throws OperationException {
        // Given
        final Iterable<Iterable<Set<Edge>>> walks = Arrays.asList(walk, walk1);

        final Map<Iterable<Iterable<Set<Edge>>>, Iterable<Edge>> map = new Map.Builder<Iterable<Iterable<Set<Edge>>>>()
                .input(walks)
                .first(new IterableFunction.Builder<Iterable<Set<Edge>>>()
                        .first(new FirstItem<>())
                        .then(new FirstItem<>())
                        .build())
                .build();

        final ToVertices toVertices = new ToVertices.Builder()
                .edgeVertices(ToVertices.EdgeVertices.SOURCE)
                .build();

        final ToSet<Object> toSet = new ToSet<>();

        final OperationChain<Set<?>> opChain = new OperationChain.Builder()
                .first(map)
                .then(toVertices)
                .then(toSet)
                .build();

        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();
        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        final OperationChainHandler<Set<?>> opChainHandler = new OperationChainHandler<>(opChainValidator, opChainOptimisers);

        given(store.handleOperation(map, context)).willReturn(Arrays.asList(EDGE_AB, EDGE_CB));
        given(store.handleOperation(toVertices, context)).willReturn(Arrays.asList("A", "C"));
        given(store.handleOperation(toSet, context)).willReturn(Sets.newHashSet("A", "C"));

        // When
        final Iterable<?> results = opChainHandler.doOperation(opChain, context, store);

        // Then
        assertThat(results, containsInAnyOrder("A", "C"));
    }

    @Test
    public void shouldProcessWalksWithEdgeExtraction() throws OperationException {
        // Given
        final Iterable<Walk> walks = Arrays.asList(walk, walk1);

        final Map<Iterable<Walk>, Iterable<Edge>> map = new Map.Builder<Iterable<Walk>>()
                .input(walks)
                .first(new IterableFunction.Builder<Walk>()
                        .first(new ExtractWalkEdgesFromHop(1))
                        .then(new FirstItem<>())
                        .build())
                .build();

        final ToVertices toVertices = new ToVertices.Builder()
                .edgeVertices(ToVertices.EdgeVertices.SOURCE)
                .build();

        final ToSet<Object> toSet = new ToSet<>();

        final OperationChain<Set<?>> opChain = new OperationChain.Builder()
                .first(map)
                .then(toVertices)
                .then(toSet)
                .build();

        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();
        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        final OperationChainHandler<Set<?>> opChainHandler = new OperationChainHandler<>(opChainValidator, opChainOptimisers);

        given(store.handleOperation(map, context)).willReturn(Arrays.asList(EDGE_BC, EDGE_BD));
        given(store.handleOperation(toVertices, context)).willReturn(Arrays.asList("B", "B"));
        given(store.handleOperation(toSet, context)).willReturn(Sets.newHashSet("B", "B"));

        // When
        final Iterable<?> results = opChainHandler.doOperation(opChain, context, store);

        // Then
        assertThat(results, contains("B"));
    }

    @Test
    public void shouldBuildWithInvalidArgumentsAndFailExecution() throws OperationException {
        final List<Function> functions = new ArrayList<>();
        final Function<Long, Double> func = Double::longBitsToDouble;
        final Function<String, Integer> func1 = Integer::valueOf;

        functions.add(new ToString());
        functions.add(func);
        functions.add(func1);

        final Map map = new Map();
        map.setInput(3);
        map.setFunctions(functions);

        final MapHandler handler = new MapHandler();

        // When / Then
        try {
            final Object result = handler.doOperation(map, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("The input/output types of the functions were incompatible"));
        }
    }
}
