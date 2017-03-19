/*
 * Copyright 2016 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LimitedCloseableIterableTest {

    @Test
    public void shouldLimitResultsToFirstItem() {
        // Given
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 0;
        final int end = 1;

        // When
        final CloseableIterable<Integer> limitedValues = new LimitedCloseableIterable<>(values, start, end);

        // Then
        assertEquals(values.subList(start, end), Lists.newArrayList(limitedValues));
    }

    @Test
    public void shouldLimitResultsToLastItem() {
        // Given
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 2;
        final int end = Integer.MAX_VALUE;

        // When
        final CloseableIterable<Integer> limitedValues = new LimitedCloseableIterable<>(values, start, end);

        // Then
        assertEquals(values.subList(start, values.size()), Lists.newArrayList(limitedValues));
    }

    @Test
    public void shouldNotLimitResults() {
        // Given
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 0;
        final int end = Integer.MAX_VALUE;

        // When
        final CloseableIterable<Integer> limitedValues = new LimitedCloseableIterable<>(values, start, end);

        // Then
        assertEquals(values, Lists.newArrayList(limitedValues));
    }

    @Test
    public void shouldReturnNoValuesIfStartIsBiggerThanSize() {
        // Given
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 5;
        final int end = Integer.MAX_VALUE;

        // When
        final CloseableIterable<Integer> limitedValues = new LimitedCloseableIterable<>(values, start, end);

        // Then
        assertTrue(Lists.newArrayList(limitedValues).isEmpty());
    }

    @Test
    public void shouldThrowExceptionIfStartIsBiggerThanEnd() {
        // Given
        final List<Integer> values = Arrays.asList(0, 1, 2, 3);
        final int start = 3;
        final int end = 1;

        // When / Then
        try {
            new LimitedCloseableIterable<>(values, start, end);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }
}
