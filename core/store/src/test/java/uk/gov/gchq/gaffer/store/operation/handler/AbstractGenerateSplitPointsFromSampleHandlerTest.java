/*
 * Copyright 2020-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GenerateSplitPointsFromSample;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractGenerateSplitPointsFromSampleHandlerTest<S extends Store> {

    protected Schema schema = new Schema.Builder().build();

    @Test
    public void shouldThrowExceptionForNullInput() throws OperationException {
        // Given
        final AbstractGenerateSplitPointsFromSampleHandler<?, S> handler = createHandler();
        final GenerateSplitPointsFromSample operation = new GenerateSplitPointsFromSample.Builder<>()
                .numSplits(1)
                .build();

        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(operation, new Context(), createStore()))
                .withMessageContaining("input is required");
    }

    @Test
    public void shouldReturnEmptyCollectionIfNumSplitsIsLessThan1() throws OperationException {
        // Given
        final List<String> sample = createSampleOfSize(100);
        final AbstractGenerateSplitPointsFromSampleHandler<?, S> handler = createHandler();
        final GenerateSplitPointsFromSample operation = new GenerateSplitPointsFromSample.Builder<>()
                .input(sample)
                .numSplits(0)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertThat(splits).isEmpty();
    }

    @Test
    public void shouldCalculateRequiredNumberOfSplits() throws OperationException {

        // Given
        final int numSplits = 3;
        final List<String> sample = createSampleOfSize(numSplits * 10);

        final AbstractGenerateSplitPointsFromSampleHandler<?, S> handler = createHandler();
        final GenerateSplitPointsFromSample operation = new GenerateSplitPointsFromSample.Builder<>()
                .input(sample)
                .numSplits(numSplits)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        verifySplits(Arrays.asList(6, 14, 21), sample, splits, handler);
    }

    @Test
    public void shouldDeduplicateElements() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<String> sample = Collections.nCopies(numSplits * 10, "key1");

        final AbstractGenerateSplitPointsFromSampleHandler<?, S> handler = createHandler();
        final GenerateSplitPointsFromSample operation = new GenerateSplitPointsFromSample.Builder<>()
                .input(sample)
                .numSplits(numSplits)
                .build();

        final S store = createStore();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertThat(splits).hasSize(1);
        verifySplits(Collections.singletonList(0), sample, splits, handler);
    }

    protected abstract S createStore();

    protected abstract AbstractGenerateSplitPointsFromSampleHandler<?, S> createHandler();

    protected void verifySplits(final List<Integer> indexes, final List<String> sample, final List<?> splits, final AbstractGenerateSplitPointsFromSampleHandler<?, S> handler) throws OperationException {
        final GenerateSplitPointsFromSample operatation = new GenerateSplitPointsFromSample.Builder<>()
                .input(sample)
                .numSplits(Integer.MAX_VALUE)
                .build();
        final List<?> allElementsAsSplits = handler.doOperation(operatation, new Context(), createStore());

        final List<Object> expectedSplits = new ArrayList<>(indexes.size());
        for (final Integer index : indexes) {
            expectedSplits.add(allElementsAsSplits.get(index));
        }

        assertEquals(expectedSplits, splits);
    }

    protected List<String> createSampleOfSize(final int size) {

        return IntStream.range(0, size).mapToObj(Integer::toString).map("key"::concat).collect(Collectors.toList());
    }
}
