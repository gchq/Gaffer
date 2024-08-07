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
import java.util.Iterator;
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
class TransformIterableTest {

    @Test
    void shouldCreateIteratorThatReturnsOnlyValidStrings(@Mock final Validator<String> validator) {
        // Given
        final String item1 = "valid item 1";
        final String item2 = "invalid item 2";
        final String item3 = "valid item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);

        when(validator.validate(item1)).thenReturn(true);
        when(validator.validate(item2)).thenReturn(false);
        when(validator.validate(item3)).thenReturn(true);

        TransformIterable<String, String> iterable = null;
        Iterator<String> itr = null;
        try {
            iterable = new TransformIterableImpl(items, validator, true);
            itr = iterable.iterator();

            // Then 1st valid item
            assertThat(itr).hasNext();
            assertThat(itr.next()).isEqualTo("VALID ITEM 1");

            // Then 2nd valid item
            assertThat(itr).hasNext();
            assertThat(itr.next()).isEqualTo("VALID ITEM 3");
        } finally {
            CloseableUtil.close(itr, iterable);
        }
    }

    @Test
    void shouldThrowIAXExceptionWhenNextItemIsInvalidString(@Mock final Validator<String> validator) {
        // Given
        final String item1 = "valid item 1";
        final String item2 = "invalid item 2 invalid";
        final String item3 = "valid item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);

        when(validator.validate(item1)).thenReturn(true);
        when(validator.validate(item2)).thenReturn(false);

        TransformIterable<String, String> iterable = null;

        try {
            iterable = new TransformIterableImpl(items, validator, false);
            final Iterator<String> itr = iterable.iterator();

            // Then 1st valid item
            assertThat(itr).hasNext();
            assertThat(itr.next()).isEqualTo("VALID ITEM 1");
            assertThatIllegalArgumentException().isThrownBy(() -> itr.hasNext());
        } finally {
            CloseableUtil.close(iterable);
        }
    }

    @Test
    void shouldThrowNoElementExceptionWhenNextCalledWhenNoNextString(@Mock final Validator<String> validator) {
        // Given
        final String item1 = "item 1";
        final Iterable<String> items = Arrays.asList(item1);

        when(validator.validate(item1)).thenReturn(true);

        TransformIterable<String, String> iterable = null;
        try {
            iterable = new TransformIterableImpl(items, validator);
            final Iterator<String> itr = iterable.iterator();
            // Then 1st item
            assertThat(itr.next()).isEqualTo("ITEM 1");

            // Then 2nd item
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

        TransformIterable<String, String> iterable = null;
        try {
            iterable = new TransformIterableImpl(items, validator);
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

        TransformIterable<String, String> iterable = null;
        try {
            iterable = new TransformIterableImpl(items, new AlwaysValid<>(), false, autoClose);

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

        TransformIterable<String, String> iterable = null;
        try {
            iterable = new TransformIterableImpl(items, new AlwaysValid<>(), false, autoClose);

            // When
            Lists.newArrayList(iterable);

            // Then
            verify((Closeable) itemsIterator, never()).close();
        } finally {
            CloseableUtil.close(iterable);
        }
    }

    @Test
    void shouldThrowExceptionWithNullInput() {
        // Given
        final boolean autoClose = true;
        final AlwaysValid<String> valid = new AlwaysValid<>();

        // When/then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new TransformIterableImpl(null, valid, false, autoClose))
            .withMessage("Input iterable is required");
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldGetValidator() {
        // Given
        final boolean autoClose = true;
        final Iterable<String> items = mock(Iterable.class, Mockito.withSettings().extraInterfaces(Closeable.class));

        try (TransformIterable<String, String> iterable = new TransformIterableImpl(items, new AlwaysValid<>(), false, autoClose)) {
            // Then
            assertThat(iterable.getValidator()).isInstanceOf(AlwaysValid.class);
        }
    }

    private class TransformIterableImpl extends TransformIterable<String, String> {

        TransformIterableImpl(final Iterable<String> input, final Validator<String> validator) {
            super(input, validator);
        }

        TransformIterableImpl(final Iterable<String> input, final Validator<String> validator, final boolean skipInvalid) {
            super(input, validator, skipInvalid);
        }

        TransformIterableImpl(final Iterable<String> input, final Validator<String> validator, final boolean skipInvalid,
                              final boolean autoClose) {
            super(input, validator, skipInvalid, autoClose);
        }

        @Override
        protected String transform(final String item) {
            return item.toUpperCase();
        }
    }
}
