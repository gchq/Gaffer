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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.common.collect.Lists;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Deduplicate;
import uk.gov.gchq.gaffer.store.Context;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class DeduplicateHandlerTest {

    @Test
    public void shouldDeduplicateResults() throws OperationException {
        // Given
        final CloseableIterable<Integer> originalResults = new WrappedCloseableIterable<>(Arrays.asList(1, 2, 2, 2, 3, 4, 1, 5, 6, 7, 8, 5, 9, 1, 6, 8, 2, 10));
        final DeduplicateHandler<Integer> handler = new DeduplicateHandler<>();
        final Deduplicate<Integer> operation = mock(Deduplicate.class);

        given(operation.getInput()).willReturn(originalResults);

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Lists.newArrayList(results));
    }

    @Test
    public void shouldDeduplicateResultsAndMaintainOrder() throws OperationException {
        // Given
        final CloseableIterable<Integer> originalResults = new WrappedCloseableIterable<>(Arrays.asList(10, 9, 8, 10, 7, 8, 7, 6, 6, 5, 6, 9, 4, 5, 3, 4, 2, 2, 2, 1, 1));
        final DeduplicateHandler<Integer> handler = new DeduplicateHandler<>();
        final Deduplicate<Integer> operation = mock(Deduplicate.class);

        given(operation.getInput()).willReturn(originalResults);

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertEquals(Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 2, 1), Lists.newArrayList(results));
    }
}