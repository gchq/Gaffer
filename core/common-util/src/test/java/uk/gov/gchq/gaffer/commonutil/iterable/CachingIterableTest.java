/*
 * Copyright 2018-2021 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CachingIterableTest {

    private static final List<Integer> SMALL_LIST = Arrays.asList(0, 1, 2, 3, 4);
    private static final List<Integer> LARGE_LIST = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    @Test
    public void shouldCacheSmallIterable(@Mock final Iterable<Integer> mockIterable) {
        when(mockIterable.iterator())
                .thenReturn(SMALL_LIST.iterator())
                .thenReturn(SMALL_LIST.iterator());

        Iterable<Integer> cachingIterable = null;
        try {
            cachingIterable = new CachingIterable<>(mockIterable, 5);

            assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));
            assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));
            verify(mockIterable, times(1)).iterator();
        } finally {
            CloseableUtil.close(cachingIterable);
        }
    }

    @Test
    public void shouldNotCacheALargeIterable(@Mock final Iterable<Integer> mockIterable) {
        when(mockIterable.iterator())
                .thenReturn(LARGE_LIST.iterator())
                .thenReturn(LARGE_LIST.iterator());

        Iterable<Integer> cachingIterable = null;
        try {
            cachingIterable = new CachingIterable<>(mockIterable, 5);

            assertEquals(LARGE_LIST, Lists.newArrayList(cachingIterable));
            assertEquals(LARGE_LIST, Lists.newArrayList(cachingIterable));
            verify(mockIterable, times(2)).iterator();
        } finally {
            CloseableUtil.close(cachingIterable);
        }
    }

    @Test
    public void shouldHandleNullIterable() {
        Iterable<Integer> cachingIterable = null;
        try {
            cachingIterable = new CachingIterable<>(null);

            assertEquals(Collections.emptyList(), Lists.newArrayList(cachingIterable));
            assertEquals(Collections.emptyList(), Lists.newArrayList(cachingIterable));
        } finally {
            CloseableUtil.close(cachingIterable);
        }
    }

    @Test
    public void shouldCloseTheIterable() throws IOException {
        @SuppressWarnings("unchecked")
        final Iterable<Integer> iterable = Mockito.mock(Iterable.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));

        CachingIterable<Integer> cachingIterable = null;
        try {
            cachingIterable = new CachingIterable<>(iterable, 5);

            cachingIterable.close();

            verify((Closeable) iterable).close();
        } finally {
            CloseableUtil.close(cachingIterable);
        }
    }

    @Test
    public void shouldCloseTheIterableWhenFullyCached() throws IOException {
        @SuppressWarnings("unchecked")
        final Iterable<Integer> iterable = Mockito.mock(Iterable.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));
        when(iterable.iterator()).thenReturn(SMALL_LIST.iterator());

        Iterable<Integer> cachingIterable = null;
        try {
            cachingIterable = new CachingIterable<>(iterable, 5);

            assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));
            verify((Closeable) iterable).close();
        } finally {
            CloseableUtil.close(cachingIterable);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldHandleMultipleIterators() throws IOException {
        // Given
        final Iterable<Integer> iterable = Mockito.mock(Iterable.class,
                Mockito.withSettings().extraInterfaces(Closeable.class));

        when(iterable.iterator()).thenReturn(
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator(),
                SMALL_LIST.iterator());

        Iterable<Integer> cachingIterable = null;
        Iterator<Integer> itr3 = null;
        Iterator<Integer> itr4 = null;
        Iterator<Integer> itr5 = null;
        try {
            cachingIterable = new CachingIterable<>(iterable, 5);

            // When / Then
            final Iterator<Integer> itr1 = cachingIterable.iterator();
            itr1.next();
            final Iterator<Integer> itr2 = cachingIterable.iterator();
            CloseableUtil.close(itr1);
            itr2.next();
            CloseableUtil.close(itr2);
            itr3 = cachingIterable.iterator();
            itr3.next();

            itr4 = cachingIterable.iterator();
            assertEquals(SMALL_LIST, Lists.newArrayList(itr4));

            // should be cached now as it has been fully read.
            verify((Closeable) iterable, times(3)).close();

            itr3.next();

            verify(iterable, times(4)).iterator();
            assertEquals(SMALL_LIST, Lists.newArrayList(cachingIterable));

            itr5 = cachingIterable.iterator();
            assertThat(itr5.next()).isEqualTo((Integer) 0);
            verify(iterable, times(4)).iterator();
        } finally {
            CloseableUtil.close(cachingIterable, itr3, itr4, itr5);
        }
    }
}
