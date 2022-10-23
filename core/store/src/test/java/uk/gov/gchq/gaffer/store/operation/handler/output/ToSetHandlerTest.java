/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class ToSetHandlerTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldConvertIterableToSet(@Mock final ToSet<Integer> operation) throws OperationException {
        // Given
        final Iterable<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final Iterable<Integer> originalResults = Arrays.asList(1, 2, 2, 2, 3, 4, 1, 5, 6, 7, 8, 5, 9, 1, 6, 8, 2, 10);
        final ToSetHandler<Integer> handler = new ToSetHandler<>();

        given(operation.getInput()).willReturn((Iterable) originalResults);

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertThat(results).containsExactlyElementsOf(expected);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldConvertIterableToSetAndMaintainOrder(@Mock final ToSet<Integer> operation)
            throws OperationException {
        // Given
        final Iterable<Integer> expected = Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 2, 1);
        final Iterable<Integer> originalResults = Arrays.asList(10, 9, 8, 10, 7, 8, 7, 6, 6, 5, 6, 9, 4, 5, 3, 4, 2, 2, 2, 1, 1);
        final ToSetHandler<Integer> handler = new ToSetHandler<>();

        given(operation.getInput()).willReturn((Iterable) originalResults);

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertThat(results).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldHandleNullInput(@Mock final ToSet<Integer> operation) throws OperationException {
        // Given
        final ToSetHandler<Integer> handler = new ToSetHandler<>();

        given(operation.getInput()).willReturn(null);

        // When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertThat(results).isNull();
    }
}
