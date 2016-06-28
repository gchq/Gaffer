/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.operation.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.operation.Operation;
import gaffer.operation.OperationException;
import gaffer.store.Context;
import gaffer.user.User;
import org.junit.Test;
import java.util.Arrays;

public class AccumuloGetIterableHandlerTest {

    @Test
    public void shouldDeduplicateResultsInHashSetWhenOperationOptionIsProvided() throws OperationException {
        // Given
        final Iterable<Integer> originalResults = Arrays.asList(1, 2, 2, 2, 3, 4, 1, 5, 6, 7, 8, 5, 9, 1, 6, 8, 2, 10);
        final AccumuloGetIterableHandlerImpl handler = new AccumuloGetIterableHandlerImpl(originalResults);
        final Operation<Void, Iterable<Integer>> operation = mock(Operation.class);

        given(operation.getOption(AccumuloStoreConstants.DEDUPLICATE_RESULTS)).willReturn("true");

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Lists.newArrayList(results));
    }

    @Test
    public void shouldDeduplicateResultsInHashSetWhenOperationOptionIsProvidedAndMaintainOrder() throws OperationException {
        // Given
        final Iterable<Integer> originalResults = Arrays.asList(10, 9, 8, 10, 7, 8, 7, 6, 6, 5, 6, 9, 4, 5, 3, 4, 2, 2, 2, 1, 1);
        final AccumuloGetIterableHandlerImpl handler = new AccumuloGetIterableHandlerImpl(originalResults);
        final Operation<Void, Iterable<Integer>> operation = mock(Operation.class);

        given(operation.getOption(AccumuloStoreConstants.DEDUPLICATE_RESULTS)).willReturn("true");

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertEquals(Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 2, 1), Lists.newArrayList(results));
    }

    @Test
    public void shouldNotDeduplicateResultWhenOperationOptionIsNotProvided() throws OperationException {
        // Given
        final Iterable<Integer> originalResults = Arrays.asList(1, 2, 2, 2, 3, 4, 1, 5, 6, 7, 8, 5, 9, 1, 6, 8, 2, 10);
        final AccumuloGetIterableHandlerImpl handler = new AccumuloGetIterableHandlerImpl(originalResults);
        final Operation<Void, Iterable<Integer>> operation = mock(Operation.class);

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertSame(originalResults, results);
    }

    @Test
    public void shouldNotDeduplicateResultWhenOperationOptionIsSetToFalse() throws OperationException {
        // Given
        final Iterable<Integer> originalResults = Arrays.asList(1, 2, 2, 2, 3, 4, 1, 5, 6, 7, 8, 5, 9, 1, 6, 8, 2, 10);
        final AccumuloGetIterableHandlerImpl handler = new AccumuloGetIterableHandlerImpl(originalResults);
        final Operation<Void, Iterable<Integer>> operation = mock(Operation.class);

        given(operation.getOption(AccumuloStoreConstants.DEDUPLICATE_RESULTS)).willReturn("false");

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertSame(originalResults, results);
    }

    private static class AccumuloGetIterableHandlerImpl extends AccumuloGetIterableHandler<Operation<Void, Iterable<Integer>>, Integer> {
        private final Iterable<Integer> results;

        public AccumuloGetIterableHandlerImpl(final Iterable<Integer> results) {
            this.results = results;
        }

        @Override
        protected Iterable<Integer> doOperation(final Operation<Void, Iterable<Integer>> operation, final User user, final AccumuloStore store) throws OperationException {
            return results;
        }
    }
}