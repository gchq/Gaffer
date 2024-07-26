/*
 * Copyright 2018-2024 Crown Copyright
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingIterableTest {

    private static final List<Integer> SMALL_LIST = Arrays.asList(0, 1, 2, 3, 4);
    private static final List<Integer> LARGE_LIST = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    @Test
    void shouldCacheSmallIterable(@Mock final Iterable<Integer> mockIterable) {
        when(mockIterable.iterator())
                .thenReturn(SMALL_LIST.iterator())
                .thenReturn(SMALL_LIST.iterator());

        final Iterable<Integer> cachingIterable = new CachingIterable<>(mockIterable, 5);

        assertThat(cachingIterable)
                .containsExactlyElementsOf(SMALL_LIST)
                .containsExactlyElementsOf(SMALL_LIST);
        verify(mockIterable, times(1)).iterator();
    }

    @Test
    void shouldNotCacheALargeIterable(@Mock final Iterable<Integer> mockIterable) {
        when(mockIterable.iterator())
                .thenReturn(LARGE_LIST.iterator())
                .thenReturn(LARGE_LIST.iterator());

        final Iterable<Integer> cachingIterable = new CachingIterable<>(mockIterable, 5);

        assertThat(cachingIterable)
                .containsExactlyElementsOf(LARGE_LIST)
                .containsExactlyElementsOf(LARGE_LIST);
        verify(mockIterable, times(2)).iterator();
    }

    @Test
    void shouldHandleNullIterable() {
        Iterable<Integer> cachingIterable = new CachingIterable<>(null);

        assertThat(cachingIterable).isEmpty();
    }

    @Test
    void shouldCloseTheIterable() throws IOException {
        @SuppressWarnings("unchecked")
        final Iterable<Integer> iterable = Mockito.mock(Iterable.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));

        final CachingIterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        cachingIterable.close();
        verify((Closeable) iterable).close();
    }

    @Test
    void shouldCloseTheIterableWhenFullyCached() throws IOException {
        @SuppressWarnings("unchecked")
        final Iterable<Integer> iterable = Mockito.mock(Iterable.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));
        when(iterable.iterator()).thenReturn(SMALL_LIST.iterator());

        final Iterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        assertThat(cachingIterable).containsExactlyElementsOf(SMALL_LIST);
        verify((Closeable) iterable).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldHandleMultipleIterators() throws IOException {
        // Given
        final Iterable<Integer> iterable = Mockito.mock(Iterable.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));

        when(iterable.iterator()).thenReturn(
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator());

        final Iterable<Integer> cachingIterable = new CachingIterable<>(iterable, 5);

        // When / Then
        final Iterator<Integer> itr1 = cachingIterable.iterator();
        itr1.next();
        final Iterator<Integer> itr2 = cachingIterable.iterator();
        CloseableUtil.close(itr1);
        itr2.next();
        CloseableUtil.close(itr2);

        final Iterator<Integer> itr3 = cachingIterable.iterator();
        itr3.next();

        final Iterator<Integer> itr4 = cachingIterable.iterator();
        assertThat(Lists.newArrayList(itr4)).containsExactlyElementsOf(SMALL_LIST);

        // should be cached now as it has been fully read.
        verify((Closeable) iterable, times(3)).close();

        itr3.next();

        verify(iterable, times(4)).iterator();
        assertThat(cachingIterable).containsExactlyElementsOf(SMALL_LIST);

        final Iterator<Integer> itr5 = cachingIterable.iterator();
        assertThat(itr5.next()).isEqualTo((Integer) 0);
        verify(iterable, times(4)).iterator();
    }
}
