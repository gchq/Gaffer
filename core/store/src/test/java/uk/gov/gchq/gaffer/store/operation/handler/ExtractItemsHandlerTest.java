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

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.ExtractItems;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class ExtractItemsHandlerTest {

    private ExtractItemsHandler handler;
    private Iterable<Iterable<?>> input;
    private Context context;
    private Store store;
    private User user;

    private final Edge edge = new Edge.Builder()
            .source("A")
            .dest("B")
            .directed(true)
            .group("road")
            .build();

    private final Edge edge1 = new Edge.Builder()
            .source("B")
            .dest("C")
            .directed(true)
            .group("road")
            .build();

    private final Edge edge2 = new Edge.Builder()
            .source("D")
            .dest("E")
            .directed(false)
            .group("path")
            .build();

    private final Edge edge3 = new Edge.Builder()
            .source("E")
            .dest("F")
            .directed(false)
            .group("path")
            .build();

    @Before
    public void setup() {
        handler = new ExtractItemsHandler();
        context = mock(Context.class);
        store = mock(Store.class);
        user = new User();

        given(context.getUser()).willReturn(user);
    }


    @Test
    public void shouldExtractFirstEdgeFromList() throws OperationException {
        // Given
        final List<Edge> firstList = Arrays.asList(edge, edge1);
        final List<Edge> secondList = Arrays.asList(edge2, edge3);

        input = Arrays.asList(firstList, secondList);

        final ExtractItems extractItems = new ExtractItems.Builder()
                .input(input)
                .selection(0)
                .build();

        final List<Edge> expected = Arrays.asList(edge, edge2);

        // When
        final List<? extends Object> results = handler.doOperation(extractItems, context, store);

        // Then
        assertEquals(2, results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldExtractSecondEdgeFromSet() throws OperationException {
        // Given
        input = new LinkedHashSet<>();

        final Set<Edge> firstSet = new LinkedHashSet<>();
        firstSet.add(edge);
        firstSet.add(edge1);

        final Set<Edge> secondSet = new LinkedHashSet<>();
        secondSet.add(edge2);
        secondSet.add(edge3);

        ((Set) input).add(firstSet);
        ((Set) input).add(secondSet);

        final ExtractItems extractItems = new ExtractItems.Builder()
                .input(input)
                .selection(1)
                .build();

        final List<Edge> expected = Arrays.asList(edge1, edge3);

        // When
        final List<? extends Object> results = handler.doOperation(extractItems, context, store);

        // Then
        assertEquals(2, results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldExecuteCorrectlyAsPartOfOperationChain() throws OperationException {
        // Given
        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();

        final OperationChainHandler<Iterable<? extends Object>> opChainHandler = new OperationChainHandler<>(opChainValidator, opChainOptimisers);

        final List<Edge> firstList = Arrays.asList(edge, edge1);
        final List<Edge> secondList = Arrays.asList(edge2, edge3);

        input = Arrays.asList(firstList, secondList);

        final Operation mockWalk = mock(Operation.class);

        final ExtractItems extractItems = new ExtractItems.Builder()
                .selection(0)
                .build();

        final List<Edge> expected = Arrays.asList(edge, edge2);

        final OperationChain<Iterable<? extends Object>> opChain = new OperationChain.Builder()
                .first(mockWalk)
                .then(extractItems)
                .build();

        given(store.handleOperation(mockWalk, context)).willReturn(input);
        given(store.handleOperation(extractItems, context)).willReturn(expected);

        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        // When
        final Iterable<? extends Object> results = opChainHandler.doOperation(opChain, context, store);

        // Then
        assertEquals(2, ((List<Edge>) results).size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldExtractItemsFromInputWithMultipleIterableTypes() throws OperationException {
        // Given
        final List<Edge> first = Arrays.asList(edge, edge1);
        final Set<Edge> second = new LinkedHashSet<>();
        second.add(edge2);
        second.add(edge3);

        input = Arrays.asList(first, second);

        final ExtractItems extractItems = new ExtractItems.Builder()
                .input(input)
                .selection(0)
                .build();

        final List<Edge> expected = Arrays.asList(edge, edge2);

        final List<? extends Object> results = handler.doOperation(extractItems, context, store);

        // Then
        assertEquals(2, results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldExtractItemsFromInputWithMultipleObjectTypes() throws OperationException {
        // Given
        final List<Edge> first = Arrays.asList(edge, edge1);
        
        final Entity entity = new Entity.Builder()
                .vertex("vertex")
                .group("map")
                .property("prop1", "value")
                .build();
        
        final Entity entity1 = new Entity.Builder()
                .vertex("vertex")
                .group("map")
                .property("prop1", "value")
                .build();
        
        final List<Entity> second = Arrays.asList(entity, entity1);
        
        input = Arrays.asList(first, second);
        
        final ExtractItems extractItems = new ExtractItems.Builder()
                .input(input)
                .selection(1)
                .build();
        
        final List<Element> expected = Arrays.asList(edge1, entity1);
        
        final List<? extends Object> results = handler.doOperation(extractItems, context, store);
        
        // Then
        assertEquals(2, results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldThrowExceptionForNullOperation() {
        // When / Then
        try {
            handler.doOperation(null, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Operation cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionForNullInput() {
        // When
        final ExtractItems operation = new ExtractItems.Builder()
                .input(null)
                .selection(1)
                .build();

        // Then
        try {
            handler.doOperation(operation, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Input cannot be null"));
        }
    }
}
