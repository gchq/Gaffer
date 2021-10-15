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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransformIterableTest {

    @Test
    public void shouldCreateIteratorThatReturnsOnlyValidStrings() {
        // Given
        final String item1 = "valid item 1";
        final String item2 = "invalid item 2";
        final String item3 = "valid item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator, true);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(false);
        given(validator.validate(item3)).willReturn(true);

        // Then 1st valid item
        assertThat(itr).hasNext();
        assertThat(itr.next()).isEqualTo("VALID ITEM 1");

        // Then 2nd valid item
        assertThat(itr).hasNext();
        assertThat(itr.next()).isEqualTo("VALID ITEM 3");
    }

    @Test
    public void shouldThrowIAXExceptionWhenNextItemIsInvalidString() {
        // Given
        final String item1 = "valid item 1";
        final String item2 = "invalid item 2 invalid";
        final String item3 = "valid item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator, false);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(false);
        given(validator.validate(item3)).willReturn(true);

        // Then 1st valid item
        assertThat(itr).hasNext();
        assertThat(itr.next()).isEqualTo("VALID ITEM 1");

        assertThatIllegalArgumentException().isThrownBy(() -> itr.hasNext());
    }

    @Test
    public void shouldThrowNoElementExceptionWhenNextCalledWhenNoNextString() {
        // Given
        final String item1 = "item 1";
        final Iterable<String> items = Arrays.asList(item1);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);

        // Then 1st item
        assertThat(itr.next()).isEqualTo("ITEM 1");

        // Then 2nd item
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> itr.next());
    }

    @Test
    public void shouldThrowExceptionIfRemoveCalled() {
        final String item1 = "item 1";
        final String item2 = "item 2";
        final Iterable<String> items = Arrays.asList(item1, item2);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(true);

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> itr.remove());
    }

    @Test
    public void shouldAutoCloseIterator() {
        // Given
        final boolean autoClose = true;
        final CloseableIterable<String> items = mock(CloseableIterable.class);
        final CloseableIterator<String> itemsIterator = mock(CloseableIterator.class);
        given(items.iterator()).willReturn(itemsIterator);
        given(itemsIterator.hasNext()).willReturn(false);

        final TransformIterableImpl iterable = new TransformIterableImpl(items, new AlwaysValid<>(), false, autoClose);

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

        final TransformIterableImpl iterable = new TransformIterableImpl(items, new AlwaysValid<>(), false, autoClose);

        // When
        Lists.newArrayList(iterable);

        // Then
        verify(itemsIterator, never()).close();
    }

    private class TransformIterableImpl extends TransformIterable<String, String> {

        TransformIterableImpl(final Iterable<String> input, final Validator<String> validator) {
            super(input, validator);
        }

        TransformIterableImpl(final Iterable<String> input, final Validator<String> validator, final boolean skipInvalid) {
            super(input, validator, skipInvalid);
        }

        TransformIterableImpl(final Iterable<String> input, final Validator<String> validator, final boolean skipInvalid, final boolean autoClose) {
            super(input, validator, skipInvalid, autoClose);
        }

        @Override
        protected String transform(final String item) {
            return item.toUpperCase();
        }
    }
}
