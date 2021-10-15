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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WrappedCloseableIterableTest {

    @Test
    public void shouldDelegateIteratorToWrappedCloseableIterable() {
        final CloseableIterable<Object> closeableIterable = mock(CloseableIterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(closeableIterable);
        final CloseableIterator<Object> closeableIterator = mock(CloseableIterator.class);
        given(closeableIterable.iterator()).willReturn(closeableIterator);

        final CloseableIterator<Object> result = wrappedIterable.iterator();

        assertEquals(closeableIterator, result);
    }

    @Test
    public void shouldDelegateIteratorToWrappedIterable() {
        final Iterable<Object> iterable = mock(Iterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(iterable);
        final Iterator<Object> iterator = mock(Iterator.class);
        given(iterable.iterator()).willReturn(iterator);

        final CloseableIterator<Object> result = wrappedIterable.iterator();

        result.hasNext();

        verify(iterator).hasNext();
    }

    @Test
    public void shouldDelegateCloseToWrappedCloseableIterable() {
        final CloseableIterable<Object> closeableIterable = mock(CloseableIterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(closeableIterable);

        wrappedIterable.close();

        verify(closeableIterable).close();
    }

    @Test
    public void shouldDoNothingWhenCloseCalledOnNoncloseableIterable() {
        final Iterable<Object> iterable = mock(Iterable.class);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(iterable);

        assertThatNoException().isThrownBy(() -> wrappedIterable.close());
    }

    @Test
    public void shouldDelegateCloseToWrappedIterables() {
        final CloseableIterable<? extends Object> iterable1 = mock(CloseableIterable.class);
        final CloseableIterable<Integer> iterable2 = mock(CloseableIterable.class);
        final Iterable<Object> chainedIterable = getChainedIterable(iterable1, iterable2);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(chainedIterable);

        wrappedIterable.close();

        verify(iterable1, Mockito.atLeastOnce()).close();
        verify(iterable2, Mockito.atLeastOnce()).close();
    }

    @Test
    public void shouldDelegateIteratorCloseToWrappedIterables() {
        final CloseableIterable<? extends Object> iterable1 = mock(CloseableIterable.class);
        final CloseableIterable<Integer> iterable2 = mock(CloseableIterable.class);
        final Iterable<Object> chainedIterable = getChainedIterable(iterable1, iterable2);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(chainedIterable);

        wrappedIterable.iterator().close();

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

        final Iterable<Object> chainedIterable = getChainedIterable(iterable1, iterable2);
        final WrappedCloseableIterable<Object> wrappedIterable = new WrappedCloseableIterable<>(chainedIterable);

        // When
        wrappedIterable.iterator().hasNext();

        // Then
        verify(iterator1, Mockito.atLeastOnce()).close();
        verify(iterator2, Mockito.atLeastOnce()).close();
    }

    private Iterable<Object> getChainedIterable(CloseableIterable<?> iterable1, CloseableIterable<Integer> iterable2) {
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
        return new ChainedIterable<>(limitedIterable, transformOneToManyIterable);
    }
}
