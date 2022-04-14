/*
 * Copyright 2016-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.iterable;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.exception.LimitExceededException;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class LimitedIterableTest {

    @Test
    public void shouldLimitResultsToFirstItem() {
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 0;
        final int end = 1;

        final Iterable<Integer> limitedValues = new LimitedIterable<>(values, start, end);
        assertThat(limitedValues).containsExactlyElementsOf(values.subList(start, end));
    }

    @Test
    public void shouldLimitResultsToLastItem() {
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 2;
        final int end = Integer.MAX_VALUE;

        final Iterable<Integer> limitedValues = new LimitedIterable<>(values, start, end);
        assertThat(limitedValues).containsExactlyElementsOf(values.subList(start, values.size()));
    }

    @Test
    public void shouldNotLimitResults() {
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 0;
        final int end = Integer.MAX_VALUE;

        final Iterable<Integer> limitedValues = new LimitedIterable<>(values, start, end);
        assertThat(limitedValues).containsExactlyElementsOf(values);
    }

    @Test
    public void shouldReturnNoValuesWhenStartIsBiggerThanSize() {
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 5;
        final int end = Integer.MAX_VALUE;

        final Iterable<Integer> limitedValues = new LimitedIterable<>(values, start, end);
        assertThat(limitedValues).isEmpty();
    }

    @Test
    public void shouldThrowIAXWhenStartIsBiggerThanEnd() {
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 3;
        final int end = 1;

        assertThatIllegalArgumentException().isThrownBy(() -> new LimitedIterable<>(values, start, end));
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldThrowExceptionWhenDataIsTruncated() {
        // Given
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 0;
        final int end = 2;
        final boolean truncate = false;

        // When
        final Iterable<Integer> limitedValues = new LimitedIterable<>(values, start, end, truncate);

        assertThatExceptionOfType(LimitExceededException.class).isThrownBy(() -> {
            for (final Integer i : limitedValues) {
                // Do nothing until LimitExceededException is thrown
            }
        }).withMessage("Limit of 2 exceeded.");

        CloseableUtil.close(limitedValues);
    }

    @Test
    public void shouldHandleNullIterable() {
        final Iterable<Integer> nullIterable = new LimitedIterable<>(null, 0, 1, true);

        assertThat(nullIterable).isEmpty();
    }

    @Test
    public void shouldHandleLimitEqualToIterableLength() {
        // Given
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 0;
        final int end = 4;
        final boolean truncate = false;

        // When
        Iterable<Integer> equalValues = new LimitedIterable<>(values, start, end, truncate);

        // Then
        assertThat(equalValues).containsExactlyElementsOf(values);
    }
}
