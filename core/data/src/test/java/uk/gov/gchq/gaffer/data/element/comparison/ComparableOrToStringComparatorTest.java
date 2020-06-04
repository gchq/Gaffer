/*
 * Copyright 2017-2020 Crown Copyright
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
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.ByteUtil;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ComparableOrToStringComparatorTest {

    @Test
    public void shouldSortValuesUsingComparator() {
        // Given
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();
        final List<Object> values = Arrays.asList(
                null, 1, "2", new IntegerWrapper(0), null, 7, "5", 3, new IntegerWrapper(4)
        );

        // When
        values.sort(comparator);

        // Then
        final List<Object> expected = Arrays.asList(
                new IntegerWrapper(0), 1, "2", 3, new IntegerWrapper(4), "5", 7, null, null
        );
        assertEquals(expected, values);
    }

    @Test
    public void shouldMatchStringComparison() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare("1", "2");

        assertTrue(result < 0, "Both should be less than 0");
    }

    @Test
    public void shouldMatchStringComparisonInReverse() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare("2", "1");

        assertTrue(result > 0, "Both should be less than 0");
    }

    @Test
    public void shouldCompareEqualStrings() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare("1", "1");

        assertEquals(0, result);
    }

    @Test
    public void shouldMatchByteComparison() {
        final byte[] bytes1 = "1".getBytes();
        final byte[] bytes2 = "2".getBytes();

        final int bytesResult = ByteUtil.compareSortedBytes(bytes1, bytes2);

        assertTrue(bytesResult < 0, "Both should be less than 0");
    }

    @Test
    public void shouldMatchByteComparisonInReverse() {
        final byte[] bytes2 = "2".getBytes();
        final byte[] bytes1 = "1".getBytes();

        final int bytesResult = ByteUtil.compareSortedBytes(bytes2, bytes1);

        assertTrue(bytesResult > 0, "Both should be less than 0");
    }

    @Test
    public void shouldCompareNulls() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(null, null);

        assertEquals(0, result);
    }

    @Test
    public void shouldCompareNullWithValue() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(null, 1);

        assertTrue(result > 0, "Should be more than 0");
    }

    @Test
    public void shouldCompareValueWithNull() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(1, null);

        assertTrue(result < 0, "Should be less than 0");
    }

    @Test
    public void shouldCompareIntegers() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(1, 2);

        assertTrue(result < 0, "Should be less than 0");
    }

    @Test
    public void shouldCompareIntegersReversed() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(2, 1);

        assertTrue(result > 0, "Should be more than 0");
    }

    @Test
    public void shouldCompareEqualIntegers() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(1, 1);

        assertEquals(0, result);
    }

    @Test
    public void shouldCompareEqualArrayWithString() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(new Integer[] {1, 1}, "[1, 1]");

        assertEquals(0, result);
    }

    @Test
    public void shouldCompareIntegerArrays() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(new Integer[] {1, 1}, new Integer[] {1, 2});

        assertTrue(result < 0, "Should be less than 0");
    }

    @Test
    public void shouldCompareIntegerArraysReversed() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(new Integer[] {1, 2}, new Integer[] {1, 1});

        assertTrue(result > 0, "Should be more than 0");
    }

    @Test
    public void shouldCompareEqualIntegerArrays() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(new Integer[] {1, 2}, new Integer[] {1, 2});

        assertEquals(0, result);
    }

    @Test
    public void shouldCompareCustomObjUsingToString() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(new IntegerWrapper(1), new IntegerWrapper(2));

        assertTrue(result < 0, "Should be less than 0");
    }

    @Test
    public void shouldCompareCustomObjUsingToStringReversed() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(new IntegerWrapper(2), new IntegerWrapper(1));

        assertTrue(result > 0, "Should be more than 0");
    }

    @Test
    public void shouldCompareEqualCustomObjUsingToString() {
        final ComparableOrToStringComparator comparator = new ComparableOrToStringComparator();

        final int result = comparator.compare(new IntegerWrapper(1), new IntegerWrapper(1));

        assertEquals(0, result);
    }

    private static final class IntegerWrapper {
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
