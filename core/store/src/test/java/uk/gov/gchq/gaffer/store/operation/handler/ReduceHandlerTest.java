/*
 * Copyright 2016-2019 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Reduce;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ReduceHandlerTest {
    @Test
    public void shouldAggregateResults() throws Exception {
        // Given
        final List<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
        final Integer expectedResult = 15;
        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .aggregateFunction(new Sum())
                .build();

        final ReduceHandler<Integer> handler = new ReduceHandler<>();

        // When
        final Integer result = handler.doOperation(reduce, null, null);

        // Then
        assertTrue(result instanceof Integer);
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldAggregateResultsWithIdentity() throws Exception {
        // Given
        final List<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
        final Integer identity = 10;
        final Integer expectedResult = 10;
        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .identity(10)
                .aggregateFunction(new Max())
                .build();

        final ReduceHandler<Integer> handler = new ReduceHandler<>();

        // When
        final Integer result = handler.doOperation(reduce, null, null);

        // Then
        assertTrue(result instanceof Integer);
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldAggregateResultsWithNullIdentity() throws Exception {
        // Given
        final List<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
        final Integer expectedResult = 5;
        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .identity(null)
                .aggregateFunction(new Max())
                .build();

        final ReduceHandler<Integer> handler = new ReduceHandler<>();

        // When
        final Integer result = handler.doOperation(reduce, null, null);

        // Then
        assertTrue(result instanceof Integer);
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldHandleNullInput() throws Exception {
        // Given
        final Iterable<Integer> input = null;
        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .build();

        final ReduceHandler<Integer> handler = new ReduceHandler<>();

        // When
        try {
            final Integer result = handler.doOperation(reduce, null, null);
        } catch (final OperationException oe) {

            // Then
            assertThat(oe.getMessage(), is("Input cannot be null"));
        }
    }

    @Test
    public void shouldHandleNullOperation() throws Exception {
        // Given
        final ReduceHandler<Integer> handler = new ReduceHandler<>();

        // When
        try {
            final Integer result = handler.doOperation(null, null, null);
        } catch (final OperationException oe) {

            // Then
            assertThat(oe.getMessage(), is("Operation cannot be null"));
        }
    }
}
