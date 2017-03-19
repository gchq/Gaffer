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

import org.junit.Test;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WrappedCloseableIterableTest {

    @Test
    public void shouldDelegateIteratorToWrappedCloseableIterable() {
        // Given
        final CloseableIterable<Object> closeableIterable = mock(CloseableIterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(closeableIterable);
        final CloseableIterator<Object> closeableIterator = mock(CloseableIterator.class);
        given(closeableIterable.iterator()).willReturn(closeableIterator);

        // When
        final CloseableIterator<Object> result = wrappedIterable.iterator();

        // Then
        assertEquals(closeableIterator, result);
    }

    @Test
    public void shouldDelegateIteratorToWrappedIterable() {
        // Given
        final Iterable<Object> iterable = mock(Iterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(iterable);
        final Iterator<Object> iterator = mock(Iterator.class);
        given(iterable.iterator()).willReturn(iterator);

        // When
        final CloseableIterator<Object> result = wrappedIterable.iterator();

        // Then - call has next and check it was called on the mock.
        result.hasNext();
        verify(iterator).hasNext();
    }

    @Test
    public void shouldDelegateCloseToWrappedCloseableIterable() {
        // Given
        final CloseableIterable<Object> closeableIterable = mock(CloseableIterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(closeableIterable);

        // When
        wrappedIterable.close();

        // Then
        verify(closeableIterable).close();
    }

    @Test
    public void shouldDoNothingWhenCloseCalledOnNoncloseableIterable() {
        // Given
        final Iterable<Object> iterable = mock(Iterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(iterable);

        // When
        wrappedIterable.close();

        // Then - no exception
    }
}
