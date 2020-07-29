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

package uk.gov.gchq.gaffer.commonutil.iterable;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransformOneToManyIterableTest {

    @Test
    public void shouldCreateIteratorThatReturnsOnlyValidStrings() {
        // Given
        final String nullItem0 = null;
        final String validItem1 = "item 1";
        final String invalidItem2 = "item2a,item2b";
        final String validItems3A_3B = "item 3a,item 3b";
        final String validItem4 = "item 4";
        final Iterable<String> items = Arrays.asList(nullItem0, validItem1, invalidItem2, validItems3A_3B, validItem4);
        final Validator<String> validator = mock(Validator.class);
        final TransformOneToManyIterable iterable = new TransformOneToManyIterableImpl(items, validator, true);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(nullItem0)).willReturn(true);
        given(validator.validate(validItem1)).willReturn(true);
        given(validator.validate(invalidItem2)).willReturn(false);
        given(validator.validate(validItems3A_3B)).willReturn(true);
        given(validator.validate(validItem4)).willReturn(true);

        // When
        final List<String> output = Lists.newArrayList(itr);

        // Then
        final List<String> expected = Arrays.asList("ITEM 1", "ITEM 3A", "ITEM 3B", "ITEM 4");
        assertEquals(expected, output);
    }

    @Test
    public void shouldCreateIteratorThatThrowsExceptionOnInvalidString() {
        // Given
        final String item1 = "item 1";
        final String item2 = "item 2a invalid,item 2b";
        final String item3 = "item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);
        final Validator<String> validator = mock(Validator.class);
        final TransformOneToManyIterable iterable = new TransformOneToManyIterableImpl(items, validator, false);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(false);
        given(validator.validate(item3)).willReturn(true);

        // Then 1st item
        assertTrue(itr.hasNext());
        assertEquals("ITEM 1", itr.next());

        // Then 2nd item
        assertThrows(IllegalArgumentException.class, () -> itr.hasNext());
    }

    @Test
    public void shouldThrowExceptionIfNextCalledWhenNoNextString() {
        // Given
        final String item1 = "item 1";
        final String items2A_B = "item 2a,item 2b";
        final Iterable<String> items = Arrays.asList(item1, items2A_B);
        final Validator<String> validator = mock(Validator.class);
        final TransformOneToManyIterable iterable = new TransformOneToManyIterableImpl(items, validator);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(items2A_B)).willReturn(true);

        // Then iterations 1-3
        assertEquals("ITEM 1", itr.next());
        assertEquals("ITEM 2A", itr.next());
        assertEquals("ITEM 2B", itr.next());

        // Then 4th iteration
        assertThrows(NoSuchElementException.class, () -> itr.next());
    }

    @Test
    public void shouldThrowExceptionIfRemoveCalled() {
        final String item1 = "item 1";
        final String item2 = "item 2";
        final Iterable<String> items = Arrays.asList(item1, item2);
        final Validator<String> validator = mock(Validator.class);
        final TransformOneToManyIterable iterable = new TransformOneToManyIterableImpl(items, validator);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(true);

        assertThrows(UnsupportedOperationException.class, () -> itr.remove());
    }

    @Test
    public void shouldAutoCloseIterator() {
        // Given
        final boolean autoClose = true;
        final CloseableIterable<String> items = mock(CloseableIterable.class);
        final CloseableIterator<String> itemsIterator = mock(CloseableIterator.class);
        given(items.iterator()).willReturn(itemsIterator);
        given(itemsIterator.hasNext()).willReturn(false);

        final TransformOneToManyIterableImpl iterable = new TransformOneToManyIterableImpl(items, autoClose);

        // When
        Lists.newArrayList(iterable);

        // Then
        verify(itemsIterator, times(1)).close();
    }

    @Test
    public void shouldNotAutoCloseIterator() {
        // Given
        final boolean autoClose = false;
        final CloseableIterable<String> items = mock(CloseableIterable.class);
        final CloseableIterator<String> itemsIterator = mock(CloseableIterator.class);
        given(items.iterator()).willReturn(itemsIterator);
        given(itemsIterator.hasNext()).willReturn(false);

        final TransformOneToManyIterableImpl iterable = new TransformOneToManyIterableImpl(items, autoClose);

        // When
        Lists.newArrayList(iterable);

        // Then
        verify(itemsIterator, never()).close();
    }

    private class TransformOneToManyIterableImpl extends TransformOneToManyIterable<String, String> {
        TransformOneToManyIterableImpl(final Iterable<String> input, final boolean autoClose) {
            super(input, new AlwaysValid<>(), false, autoClose);
        }

        TransformOneToManyIterableImpl(final Iterable<String> input, final Validator<String> validator) {
            super(input, validator);
        }

        TransformOneToManyIterableImpl(final Iterable<String> input, final Validator<String> validator, final boolean skipInvalid) {
            super(input, validator, skipInvalid);
        }

        /**
         * Converts to upper case and splits on commas.
         *
         * @param item the I item to be transformed
         * @return the upper case and split on commas output.
         */
        @Override
        protected Iterable<String> transform(final String item) {
            if (null == item) {
                return Collections.emptyList();
            }

            return Arrays.asList(item.toUpperCase().split(","));
        }
    }
}
