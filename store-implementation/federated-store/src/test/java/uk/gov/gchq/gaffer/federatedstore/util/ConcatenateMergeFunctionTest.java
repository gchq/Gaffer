/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;

class ConcatenateMergeFunctionTest {

    @Test
    public void shouldReturnEmptyListWhenUpdateAndStateNull() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Iterable<Object> results = mergeFunction.apply(null, null);

        // Then
        assertThat(results)
                .isNotNull()
                .isEmpty();
    }

    @Test
    public void shouldReturnStateWhenUpdateNull() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Iterable<Object> state = Collections.singletonList(STRING);
        Iterable<Object> results = mergeFunction.apply(null, state);

        // Then
        assertThat(results)
                .containsExactlyElementsOf(state);
    }

    @Test
    public void shouldReturnUpdateWhenStateNull() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Iterable<Object> update = Collections.singletonList(STRING);
        Iterable<Object> results = mergeFunction.apply(update, null);

        // Then
        assertThat(results)
                .containsExactlyElementsOf(update);
    }

    @Test
    public void shouldReturnStringUpdateWrappedInIterableWhenStateNull() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Object update = STRING;
        Iterable<Object> results = mergeFunction.apply(update, null);

        // Then
        assertThat(results)
                .hasSameClassAs(results)
                .containsExactly(update);
    }

    @Test
    public void shouldReturnIntegerUpdateWrappedInIterableWhenStateNull() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Object update = 25;
        Iterable<Object> results = mergeFunction.apply(update, null);

        // Then
        assertThat(results)
                .hasSameClassAs(results)
                .containsExactly(update);
    }

    @Test
    public void shouldReturnArrayConvertedToIterableWhenStateNull() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Object[] update = {STRING, STRING};
        Iterable<Object> results = mergeFunction.apply(update, null);

        // Then
        assertThat(results)
                .hasSameClassAs(results)
                .containsExactly(STRING, STRING);
    }

    @Test
    public void shouldConcatenateTwoSingleObjectIterablesIntoOne() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Iterable<Object> update = Collections.singletonList(STRING);
        Iterable<Object> state = Collections.singletonList(STRING);
        Iterable<Object> results = mergeFunction.apply(update, state);

        // Then
        assertThat(results)
                .containsExactly(STRING, STRING);
    }

    @Test
    public void shouldConcatenateTwoMultiObjectIterablesIntoOne() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Iterable<Object> update = Arrays.asList(STRING, STRING);
        Iterable<Object> state = Collections.singletonList(STRING);
        Iterable<Object> results = mergeFunction.apply(update, state);

        // Then
        assertThat(results)
                .containsExactly(STRING, STRING, STRING);
    }

    @Test
    public void shouldWrapUpdateInIterableAndConcatenate() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Object update = STRING;
        Iterable<Object> state = Collections.singletonList(STRING);
        Iterable<Object> results = mergeFunction.apply(update, state);

        // Then
        assertThat(results)
                .containsExactly(STRING, STRING);
    }

    @Test
    public void shouldConcatenateTwoIterablesWithNullIntoOne() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Object update = Collections.singletonList(null);
        Iterable<Object> state = Collections.singletonList(null);
        Iterable<Object> results = mergeFunction.apply(update, state);

        // Then
        assertThat(results)
                .containsExactly(null, null);
    }

    @Test
    public void shouldConvertArrayToIterableAndConcatenate() {
        // Given
        final ConcatenateMergeFunction mergeFunction = new ConcatenateMergeFunction();

        // When
        Object[] update = {STRING, STRING};
        Iterable<Object> state = Collections.singletonList(STRING);
        Iterable<Object> results = mergeFunction.apply(update, state);

        // Then
        assertThat(results)
                .containsExactly(STRING, STRING, STRING);
    }
}
