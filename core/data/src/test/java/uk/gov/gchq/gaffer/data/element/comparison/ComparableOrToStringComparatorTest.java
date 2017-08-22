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
package uk.gov.gchq.gaffer.data.element.comparison;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.ByteUtil;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ComparableOrToStringComparatorTest {
    @Test
    public void shouldSortValuesUsingComparator() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();
        final List<Object> values = Arrays.asList(
                null,
                1,
                "2",
                new IntegerWrapper(0),
                null,
                7,
                "5",
                3,
                new IntegerWrapper(4)
        );

        // When
        values.sort(comparator);

        // Then
        assertEquals(
                Arrays.asList(
                        new IntegerWrapper(0),
                        1,
                        "2",
                        3,
                        new IntegerWrapper(4),
                        "5",
                        7,
                        null,
                        null
                ), values);
    }

    @Test
    public void shouldMatchByteComparison() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final String item1 = "1";
        final String item2 = "2";

        // When
        final int result = comparator.compare(item1, item2);
        final int bytesResult = ByteUtil.compareSortedBytes(item1.getBytes(), item2.getBytes());

        // Then
        assertTrue("Both should be less than 0", result < 0 && bytesResult < 0);
    }

    @Test
    public void shouldMatchByteComparisonReversed() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final String item1 = "2";
        final String item2 = "1";

        // When
        final int result = comparator.compare(item1, item2);
        final int bytesResult = ByteUtil.compareSortedBytes(item1.getBytes(), item2.getBytes());

        // Then
        assertTrue("Both should be more than 0", result > 0 && bytesResult > 0);
    }

    @Test
    public void shouldCompareNulls() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(null, null);

        // Then
        assertEquals(0, result);
    }

    @Test
    public void shouldCompareNullWithValue() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(null, 1);

        // Then
        assertTrue("Should be more than 0", result > 0);
    }

    @Test
    public void shouldCompareValueWithNull() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(1, null);

        // Then
        assertTrue("Should be less than 0", result < 0);
    }

    @Test
    public void shouldCompareStrings() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare("1", "2");

        // Then
        assertTrue("Should be less than 0", result < 0);
    }

    @Test
    public void shouldCompareStringsReversed() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare("2", "1");

        // Then
        assertTrue("Should be more than 0", result > 0);
    }

    @Test
    public void shouldCompareEqualStrings() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare("1", "1");

        // Then
        assertEquals(0, result);
    }

    @Test
    public void shouldCompareIntegers() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(1, 2);

        // Then
        assertTrue("Should be less than 0", result < 0);
    }

    @Test
    public void shouldCompareIntegersReversed() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(2, 1);

        // Then
        assertTrue("Should be more than 0", result > 0);
    }

    @Test
    public void shouldCompareEqualIntegers() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(1, 1);

        // Then
        assertEquals(0, result);
    }

    @Test
    public void shouldCompareEqualArrayWithString() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(new Integer[]{1, 1}, "[1, 1]");

        // Then
        assertEquals(0, result);
    }

    @Test
    public void shouldCompareIntegerArrays() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(new Integer[]{1, 1}, new Integer[]{1, 2});

        // Then
        assertTrue("Should be less than 0", result < 0);
    }

    @Test
    public void shouldCompareIntegerArraysReversed() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(new Integer[]{1, 2}, new Integer[]{1, 1});

        // Then
        assertTrue("Should be more than 0", result > 0);
    }

    @Test
    public void shouldCompareEqualIntegerArrays() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(new Integer[]{1, 2}, new Integer[]{1, 2});

        // Then
        assertEquals(0, result);
    }

    @Test
    public void shouldCompareCustomObjUsingToString() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(new IntegerWrapper(1), new IntegerWrapper(2));

        // Then
        assertTrue("Should be less than 0", result < 0);
    }

    @Test
    public void shouldCompareCustomObjUsingToStringReversed() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(new IntegerWrapper(2), new IntegerWrapper(1));

        // Then
        assertTrue("Should be more than 0", result > 0);
    }

    @Test
    public void shouldCompareEqualCustomObjUsingToString() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        // When
        final int result = comparator.compare(new IntegerWrapper(1), new IntegerWrapper(1));

        // Then
        assertEquals(0, result);
    }

    private final static class IntegerWrapper {
        private Integer field;

        private IntegerWrapper(final Integer field) {
            this.field = field;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            final IntegerWrapper intWrapper = (IntegerWrapper) obj;

            return new EqualsBuilder()
                    .append(field, intWrapper.field)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(13, 41)
                    .append(field)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return field.toString();
        }
    }
}
