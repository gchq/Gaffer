/*
 * Copyright 2016-2018 Crown Copyright
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
import org.mockito.Mockito;

import java.util.Collections;
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

    @Test
    public void shouldDelegateCloseToWrappedIterables() {
        // Given
        final CloseableIterable<? extends Object> iterable1 = mock(CloseableIterable.class);
        final CloseableIterable<Integer> iterable2 = mock(CloseableIterable.class);
        final Iterable limitedIterable = new LimitedCloseableIterable<>(iterable1, 0, 1);
        final Iterable<String> transformIterable = new TransformIterable<Integer, String>(iterable2) {
            @Override
            protected String transform(final Integer item) {
                return item.toString();
            }
        };
        final Iterable transformOneToManyIterable = new TransformOneToManyIterable<String, Double>(transformIterable) {
            @Override
            protected Iterable<Double> transform(final String item) {
                return Collections.singleton(Double.parseDouble(item));
            }
        };
        final Iterable<Object> chainedIterable = new ChainedIterable<>(limitedIterable, transformOneToManyIterable);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(chainedIterable);

        // When
        wrappedIterable.close();

        // Then
        verify(iterable1, Mockito.atLeastOnce()).close();
        verify(iterable2, Mockito.atLeastOnce()).close();
    }

    @Test
    public void shouldDelegateIteratorCloseToWrappedIterables() {
        // Given
        final CloseableIterable<? extends Object> iterable1 = mock(CloseableIterable.class);
        final CloseableIterable<Integer> iterable2 = mock(CloseableIterable.class);
        final Iterable limitedIterable = new LimitedCloseableIterable<>(iterable1, 0, 1);
        final Iterable<String> transformIterable = new TransformIterable<Integer, String>(iterable2) {
            @Override
            protected String transform(final Integer item) {
                return item.toString();
            }
        };
        final Iterable transformOneToManyIterable = new TransformOneToManyIterable<String, Double>(transformIterable) {
            @Override
            protected Iterable<Double> transform(final String item) {
                return Collections.singleton(Double.parseDouble(item));
            }
        };
        final Iterable<Object> chainedIterable = new ChainedIterable<>(limitedIterable, transformOneToManyIterable);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(chainedIterable);

        // When
        wrappedIterable.iterator().close();

        // Then
        verify(iterable1, Mockito.atLeastOnce()).close();
        verify(iterable2, Mockito.atLeastOnce()).close();
    }

    @Test
    public void shouldAutoCloseWrappedIterables() {
        // Given
        final CloseableIterable<?> iterable1 = mock(CloseableIterable.class);
        final CloseableIterator iterator1 = mock(CloseableIterator.class);
        given(iterable1.iterator()).willReturn(iterator1);
        given(iterator1.hasNext()).willReturn(false);

        final CloseableIterable<Integer> iterable2 = mock(CloseableIterable.class);
        final CloseableIterator iterator2 = mock(CloseableIterator.class);
        given(iterable2.iterator()).willReturn(iterator2);
        given(iterator2.hasNext()).willReturn(false);

        final Iterable limitedIterable = new LimitedCloseableIterable<>(iterable1, 0, 1);
        final Iterable<String> transformIterable = new TransformIterable<Integer, String>(iterable2) {
            @Override
            protected String transform(final Integer item) {
                return item.toString();
            }
        };
        final Iterable transformOneToManyIterable = new TransformOneToManyIterable<String, Double>(transformIterable) {
            @Override
            protected Iterable<Double> transform(final String item) {
                return Collections.singleton(Double.parseDouble(item));
            }
        };
        final Iterable<Object> chainedIterable = new ChainedIterable<>(limitedIterable, transformOneToManyIterable);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(chainedIterable);

        // When
        wrappedIterable.iterator().hasNext();

        // Then
        verify(iterator1, Mockito.atLeastOnce()).close();
        verify(iterator2, Mockito.atLeastOnce()).close();
    }
}
