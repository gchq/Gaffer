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

package uk.gov.gchq.gaffer.commonutil;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CachingIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CachingIterableTest {
    private static final List<Integer> SMALL_LIST = Arrays.asList(0, 1, 2, 3, 4);
    private static final List<Integer> LARGE_LIST = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    @Test
    public void shouldCacheSmallIterable() {
        // Given
        final CloseableIterable<Integer> iterable = mock(CloseableIterable.class);
        given(iterable.iterator()).willReturn(
                new WrappedCloseableIterator<>(SMALL_LIST.iterator()),
                new WrappedCloseableIterator<>(SMALL_LIST.iterator())
        );

        // When
        final CachingIterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        // Then
        assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));
        assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));
        verify(iterable, times(1)).iterator();
    }

    @Test
    public void shouldNotCacheALargeIterable() {
        // Given
        final CloseableIterable<Integer> iterable = mock(CloseableIterable.class);
        given(iterable.iterator()).willReturn(
                new WrappedCloseableIterator<>(LARGE_LIST.iterator()),
                new WrappedCloseableIterator<>(LARGE_LIST.iterator())
        );

        // When
        final CachingIterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        // Then
        assertEquals(LARGE_LIST, Lists.newArrayList(cachingIterable));
        assertEquals(LARGE_LIST, Lists.newArrayList(cachingIterable));
        verify(iterable, times(2)).iterator();
    }

    @Test
    public void shouldHandleNullIterable() {
        // When
        final CachingIterable<Integer> cachingIterable = new CachingIterable<>(null);

        // Then
        assertEquals(Collections.emptyList(), Lists.newArrayList(cachingIterable));
        assertEquals(Collections.emptyList(), Lists.newArrayList(cachingIterable));
    }

    @Test
    public void shouldCloseTheIterable() {
        // Given
        final CloseableIterable<Integer> iterable = mock(CloseableIterable.class);
        final CachingIterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        // When
        cachingIterable.close();

        // Then
        verify(iterable).close();
    }

    @Test
    public void shouldCloseTheIterableWhenFullyCached() {
        // Given
        final CloseableIterable<Integer> iterable = mock(CloseableIterable.class);
        given(iterable.iterator()).willReturn(
                new WrappedCloseableIterator<>(SMALL_LIST.iterator()),
                new WrappedCloseableIterator<>(SMALL_LIST.iterator())
        );
        final CachingIterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        // When
        assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));

        // Then
        verify(iterable).close();
    }

    @Test
    public void shouldHandleMultipleIterators() {
        // Given
        final CloseableIterable<Integer> iterable = mock(CloseableIterable.class);
        given(iterable.iterator()).willReturn(
                new WrappedCloseableIterator<>(SMALL_LIST.iterator()),
                new WrappedCloseableIterator<>(SMALL_LIST.iterator()),
                new WrappedCloseableIterator<>(SMALL_LIST.iterator()),
                new WrappedCloseableIterator<>(SMALL_LIST.iterator()),
                new WrappedCloseableIterator<>(SMALL_LIST.iterator())
        );
        final CachingIterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        // When / Then
        final CloseableIterator<Integer> itr1 = cachingIterable.iterator();
        itr1.next();
        final CloseableIterator<Integer> itr2 = cachingIterable.iterator();
        itr1.close();
        itr2.next();
        itr1.close();
        final CloseableIterator<Integer> itr3 = cachingIterable.iterator();
        itr3.next();
        final CloseableIterator<Integer> itr4 = cachingIterable.iterator();
        assertEquals(SMALL_LIST, Lists.newArrayList(itr4));
        // should be cached now as it has been fully read.
        verify(iterable, times(3)).close();
        itr3.next();
        verify(iterable, times(4)).iterator();
        assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));
        final CloseableIterator<Integer> itr5 = cachingIterable.iterator();
        assertEquals((Integer) 0, itr5.next());
        verify(iterable, times(4)).iterator();
    }
}
