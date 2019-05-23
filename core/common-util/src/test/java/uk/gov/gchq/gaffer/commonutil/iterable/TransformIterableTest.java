/*
 * Copyright 2016-2019 Crown Copyright
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
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TransformIterableTest {

    @Test
    public void shouldCreateIteratorThatReturnsOnlyValidStrings() {
        // Given
        final String item1 = "item 1";
        final String item2 = "item 2";
        final String item3 = "item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator, true);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(false);
        given(validator.validate(item3)).willReturn(true);

        // When 1a
        final boolean hasNext1 = itr.hasNext();

        // Then 1a
        assertTrue(hasNext1);

        // When 1b
        final String next1 = itr.next();

        // Then 1b
        assertEquals(item1.toUpperCase(), next1);

        // When 2a / Then 2a
        final boolean hasNext2 = itr.hasNext();

        // Then 2a
        assertTrue(hasNext2);

        // When 2b
        final String next2 = itr.next();

        // Then 2b
        assertEquals(item3.toUpperCase(), next2);
    }

    @Test
    public void shouldCreateIteratorThatThrowsExceptionOnInvalidString() {
        // Given
        final String item1 = "item 1";
        final String item2 = "item 2 invalid";
        final String item3 = "item 3";
        final Iterable<String> items = Arrays.asList(item1, item2, item3);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator, false);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(false);
        given(validator.validate(item3)).willReturn(true);

        // When 1a
        final boolean hasNext1 = itr.hasNext();

        // Then 1a
        assertTrue(hasNext1);

        // When 1b
        final String next1 = itr.next();

        // Then 1b
        assertEquals(item1.toUpperCase(), next1);

        // When 2a / Then 2a
        try {
            itr.hasNext();
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldThrowExceptionIfNextCalledWhenNoNextString() {
        // Given
        final String item1 = "item 1";
        final String item2 = "item 2";
        final Iterable<String> items = Arrays.asList(item1, item2);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(true);

        // When 1
        final String validElm1 = itr.next();
        final String validElm2 = itr.next();

        // Then 1
        assertEquals(item1.toUpperCase(), validElm1);
        assertEquals(item2.toUpperCase(), validElm2);

        // When 2 / Then 2
        try {
            itr.next();
            fail("Exception expected");
        } catch (final NoSuchElementException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldThrowExceptionIfRemoveCalled() {
        // Given
        final String item1 = "item 1";
        final String item2 = "item 2";
        final Iterable<String> items = Arrays.asList(item1, item2);
        final Validator<String> validator = mock(Validator.class);
        final TransformIterable iterable = new TransformIterableImpl(items, validator);
        final Iterator<String> itr = iterable.iterator();

        given(validator.validate(item1)).willReturn(true);
        given(validator.validate(item2)).willReturn(true);

        // When / Then
        try {
            itr.remove();
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
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
        TransformIterableImpl() {
            super(null);
        }

        TransformIterableImpl(final Iterable<String> input) {
            super(input);
        }

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
