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

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.FlatMap;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.function.NthItem;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class FlatMapHandlerTest {
    private FlatMapHandler<Integer, Integer> handler;
    private Context context;
    private Store store;
    private Function<Iterable<Integer>, Integer> function;
    private Iterable<Iterable<Integer>> input;
    private FlatMap<Integer, Integer> operation;

    @Before
    public void setup() {
        handler = new FlatMapHandler<>();
        context = mock(Context.class);
        store = mock(Store.class);
        function = mock(Function.class);
        final List<Integer> first = Arrays.asList(1 ,2, 3);
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

}
