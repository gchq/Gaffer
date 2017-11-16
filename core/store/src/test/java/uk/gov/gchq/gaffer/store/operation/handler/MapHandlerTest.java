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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.IterableUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.graph.Walk;
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

public class MapHandlerTest {

    private Context context;
    private Store store;
    private Function<Integer, Integer> function;
    private Integer input;

    private final Edge EDGE_AB = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("B").directed(true).build();
    private final Edge EDGE_BC = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("C").directed(true).build();
    private final Edge EDGE_BD = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("D").directed(true).build();
    private final Edge EDGE_CA = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("A").directed(true).build();
    private final Edge EDGE_CB = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("B").directed(true).build();
    private final Edge EDGE_DA = new Edge.Builder().group(TestGroups.EDGE).source("D").dest("A").directed(true).build();

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

        final Map<Integer, Integer> operation = new Map.Builder<Integer, Integer>()
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
        final MapHandler<Integer, Integer> handler = new MapHandler<>();

        final Map<Integer, Integer> operation = new Map.Builder<Integer, Integer>()
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
    public void shouldReturnItemFromOperationWithMockFunction() throws OperationException {
        // Given
        final MapHandler<Integer, Integer> handler = new MapHandler<>();

        final Map<Integer, Integer> operation = new Map.Builder<Integer, Integer>()
                .input(input)
                .function(function)
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

        final Map<Integer, String> operation = new Map.Builder<Integer, String>()
                .input(7)
                .function(Object::toString)
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

        final Map<Iterable<Integer>, String> operation = new Map.Builder<Iterable<Integer>, String>()
                .input(Arrays.asList(1, 2))
                .function(Object::toString)
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

        final Map<Iterable<Integer>, Iterable<String>> operation = new Map.Builder<Iterable<Integer>, Iterable<String>>()
                .input(Arrays.asList(1, 2))
                .function(new IterableFunction<>(Object::toString))
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

        final Map<Iterable<Iterable<Integer>>, Iterable<Integer>> operation = new Map.Builder<Iterable<Iterable<Integer>>, Iterable<Integer>>()
                .input(Arrays.asList(
                        Arrays.asList(1, 2),
                        Arrays.asList(3, 4)))
                .function(new FirstItem<>())
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

        final Map<Iterable<Iterable<Integer>>, Iterable<Integer>> operation = new Map.Builder<Iterable<Iterable<Integer>>, Iterable<Integer>>()
                .input(Arrays.asList(
                        Arrays.asList(1, 2),
                        Arrays.asList(3, 4)))
                .function(new IterableConcat<>())
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

        final MapHandler<Iterable<Iterable<Integer>>, Iterable<Integer>> handler = new MapHandler<>();

        final Map<Iterable<Iterable<Integer>>, Iterable<Integer>> operation = new Map.Builder<Iterable<Iterable<Integer>>, Iterable<Integer>>()
                .input(input)
                .function(new IterableFunction<>(new NthItem<>(1)))
                .build();

        // When
        final Iterable<Integer> results = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(results);
        assertEquals(Arrays.asList(2, 5, 8), Lists.newArrayList(results));
    }

    @Test
    public void shouldProcessWalkObjects() throws OperationException {
        // Given
        final Iterable<Iterable<Set<Edge>>> walks = Arrays.asList(walk, walk1);

        final Map<Iterable<Iterable<Set<Edge>>>, Iterable<Set<Edge>>> firstMap = new Map.Builder<Iterable<Iterable<Set<Edge>>>, Iterable<Set<Edge>>>()
                .input(walks)
                .function(new IterableFunction<>(new FirstItem<>()))
                .build();

        final MapHandler<Iterable<Iterable<Set<Edge>>>, Iterable<Set<Edge>>> handler = new MapHandler<>();

        // When
        final Iterable<Set<Edge>> results = handler.doOperation(firstMap, context, store);

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

        final Map<Iterable<Iterable<Set<Edge>>>, Iterable<Set<Edge>>> firstMap = new Map.Builder<Iterable<Iterable<Set<Edge>>>, Iterable<Set<Edge>>>()
                .input(walks)
                .function(new IterableFunction<>(new FirstItem<>()))
                .build();

        final Map<Iterable<Iterable<Edge>>, Iterable<Edge>> secondMap = new Map.Builder<Iterable<Iterable<Edge>>, Iterable<Edge>>()
                .function(new IterableFunction<>(new FirstItem<>()))
                .build();

        final ToVertices toVertices = new ToVertices.Builder()
                .edgeVertices(ToVertices.EdgeVertices.SOURCE)
                .build();

        final ToSet<Object> toSet = new ToSet<>();

        final OperationChain<Set<?>> opChain = new OperationChain.Builder()
                .first(firstMap)
                .thenTypeUnsafe(secondMap)
                .then(toVertices)
                .then(toSet)
                .build();

        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();
        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        final OperationChainHandler<Set<?>> opChainHandler = new OperationChainHandler<>(opChainValidator, opChainOptimisers);

        given(store.handleOperation(firstMap, context)).willReturn(Arrays.asList(
                Sets.newHashSet(EDGE_AB),
                Sets.newHashSet(EDGE_CB)));
        given(store.handleOperation(secondMap, context)).willReturn(Arrays.asList(EDGE_AB, EDGE_CB));
        given(store.handleOperation(toVertices, context)).willReturn(Arrays.asList("A", "C"));
        given(store.handleOperation(toSet, context)).willReturn(Sets.newHashSet("A", "C"));

        // When
        final Iterable<?> results = opChainHandler.doOperation(opChain, context, store);

        // Then
        assertThat(results, containsInAnyOrder("A", "C"));
    }

    // To be removed after Koryphe 1.1.0
    private static class IterableFunction<I_ITEM, O_ITEM> implements Function<Iterable<I_ITEM>, Iterable<O_ITEM>> {
        final Function<I_ITEM, O_ITEM> delegateFunction;

        public IterableFunction(final Function<I_ITEM, O_ITEM> delegateFunction) {
            this.delegateFunction = delegateFunction;
        }

        @Override
        public Iterable<O_ITEM> apply(final Iterable<I_ITEM> i_items) {
            return IterableUtil.applyFunction(i_items, delegateFunction);
        }
    }

    // To be removed after Koryphe 1.1.0
    private static class IterableConcat<I_ITEM> implements Function<Iterable<Iterable<I_ITEM>>, Iterable<I_ITEM>> {
        @Override
        public Iterable<I_ITEM> apply(final Iterable<Iterable<I_ITEM>> i_items) {
            return Iterables.concat(i_items);
        }
    }
}
