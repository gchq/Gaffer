/*
 * Copyright 2016-2024 Crown Copyright
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransformOneToManyIterableTest {

    @Test
    void shouldCreateIteratorThatReturnsOnlyValidStrings(@Mock final Validator<String> validator) {
        // Given
        final String nullItem0 = null;
        final String validItem1 = "item 1";
        final String invalidItem2 = "item2a,item2b";
        final String validItems3A_3B = "item 3a,item 3b";
        final String validItem4 = "item 4";
        final Iterable<String> items = Arrays.asList(nullItem0, validItem1, invalidItem2, validItems3A_3B, validItem4);

        when(validator.validate(nullItem0)).thenReturn(true);
        when(validator.validate(validItem1)).thenReturn(true);
        when(validator.validate(invalidItem2)).thenReturn(false);
        when(validator.validate(validItems3A_3B)).thenReturn(true);
        when(validator.validate(validItem4)).thenReturn(true);

        TransformOneToManyIterable<String, String> iterable = null;
        Iterator<String> itr = null;
        try {
            iterable = new TransformOneToManyIterableImpl(items, validator, true);
            itr = iterable.iterator();

            // When
            final List<String> output = Lists.newArrayList(itr);

            // Then
            final List<String> expected = Arrays.asList("ITEM 1", "ITEM 3A", "ITEM 3B", "ITEM 4");
            assertThat(output).isEqualTo(expected);
        } finally {
            CloseableUtil.close(itr, iterable);
        }
    }

    @Test
    void shouldCreateIteratorThatThrowsExceptionOnInvalidString(@Mock final Validator<String> validator) {
        // Given
        final String item1 = "item 1";
        final String item2 = "item 2a invalid,item 2b";
        final String item3 = "item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);

        when(validator.validate(item1)).thenReturn(true);
        when(validator.validate(item2)).thenReturn(false);

        // Then 1st item
        TransformOneToManyIterable<String, String> iterable = null;

        try {
            iterable = new TransformOneToManyIterableImpl(items, validator, false);
            final Iterator<String> itr = iterable.iterator();
            assertThat(itr).hasNext();
            assertThat(itr.next()).isEqualTo("ITEM 1");

            // Then 2nd item
            assertThatIllegalArgumentException().isThrownBy(() -> itr.hasNext());
        } finally {
            CloseableUtil.close(iterable);
        }
    }

    @Test
    void shouldThrowExceptionIfNextCalledWhenNoNextString(@Mock final Validator<String> validator) {
        // Given
        final String item1 = "item 1";
        final String items2A_B = "item 2a,item 2b";
        final Iterable<String> items = Arrays.asList(item1, items2A_B);

        when(validator.validate(item1)).thenReturn(true);
        when(validator.validate(items2A_B)).thenReturn(true);

        TransformOneToManyIterable<String, String> iterable = null;
        try {
            iterable = new TransformOneToManyIterableImpl(items, validator);
            final Iterator<String> itr = iterable.iterator();
            // Then iterations 1-3
            assertThat(itr.next()).isEqualTo("ITEM 1");
            assertThat(itr.next()).isEqualTo("ITEM 2A");
            assertThat(itr.next()).isEqualTo("ITEM 2B");

            // Then 4th iteration
            assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> itr.next());
        } finally {
            CloseableUtil.close(iterable);
        }
    }

    @Test
    void shouldThrowExceptionIfRemoveCalled(@Mock final Validator<String> validator) {
        final String item1 = "item 1";
        final String item2 = "item 2";
        final Iterable<String> items = Arrays.asList(item1, item2);

        TransformOneToManyIterable<String, String> iterable = null;
        try {
            iterable = new TransformOneToManyIterableImpl(items, validator);

            final Iterator<String> itr = iterable.iterator();

            assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> itr.remove());
        } finally {
            CloseableUtil.close(iterable);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldAutoCloseIterator() throws IOException {
        // Given
        final boolean autoClose = true;
        final Iterable<String> items = mock(Iterable.class, Mockito.withSettings().extraInterfaces(Closeable.class));
        final Iterator<String> itemsIterator = mock(Iterator.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));
        when(items.iterator()).thenReturn(itemsIterator);
        when(itemsIterator.hasNext()).thenReturn(false);

        TransformOneToManyIterable<String, String> iterable = null;
        try {
            iterable = new TransformOneToManyIterableImpl(items, autoClose);

            // When
            Lists.newArrayList(iterable);

            // Then
            verify((Closeable) itemsIterator, times(1)).close();
        } finally {
            CloseableUtil.close(iterable);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldNotAutoCloseIterator() throws IOException {
        // Given
        final boolean autoClose = false;
        final Iterable<String> items = mock(Iterable.class, Mockito.withSettings().extraInterfaces(Closeable.class));
        final Iterator<String> itemsIterator = mock(Iterator.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));

        when(items.iterator()).thenReturn(itemsIterator);
        when(itemsIterator.hasNext()).thenReturn(false);

        TransformOneToManyIterable<String, String> iterable = null;
        try {
            iterable = new TransformOneToManyIterableImpl(items, autoClose);

            // When
            Lists.newArrayList(iterable);

            // Then
            verify((Closeable) itemsIterator, never()).close();
        } finally {
            CloseableUtil.close(iterable);
        }
    }

    private class TransformOneToManyIterableImpl extends TransformOneToManyIterable<String, String> {
        TransformOneToManyIterableImpl(final Iterable<String> input, final boolean autoClose) {
            super(input, new AlwaysValid<>(), false, autoClose);
        }

        TransformOneToManyIterableImpl(final Iterable<String> input, final Validator<String> validator) {
            super(input, validator);
        }

        TransformOneToManyIterableImpl(final Iterable<String> input, final Validator<String> validator,
                                       final boolean skipInvalid) {
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
