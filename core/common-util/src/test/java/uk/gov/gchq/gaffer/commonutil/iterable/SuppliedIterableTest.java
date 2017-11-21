/*
 * Copyright 2017 Crown Copyright
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

import java.util.Arrays;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SuppliedIterableTest {

    @Test
    public void shouldRequestNewIterableFromSupplierWhenIteratorInvoked() {
        // Given
        final Supplier<Iterable<Integer>> supplier = mock(Supplier.class);
        final Iterable<Integer> iterable1 = Arrays.asList(1, 2, 3);
        final Iterable<Integer> iterable2 = Arrays.asList(4, 5, 6);
        given(supplier.get()).willReturn(iterable1, iterable2);

        final SuppliedIterable<Integer> suppliedItr = new SuppliedIterable<>(supplier);

        // When 1
        final CloseableIterator<Integer> result1 = suppliedItr.iterator();
        final CloseableIterator<Integer> result2 = suppliedItr.iterator();

        // Then 2
        verify(supplier, times(2)).get();
        assertEquals(iterable1, Lists.newArrayList(result1));
        assertEquals(iterable2, Lists.newArrayList(result2));
    }

    @Test
    public void shouldCloseIterables() {
        // Given
        final Supplier<Iterable<Integer>> supplier = mock(Supplier.class);
        final CloseableIterable<Integer> iterable1 = mock(CloseableIterable.class);
        final CloseableIterable<Integer> iterable2 = mock(CloseableIterable.class);
        final CloseableIterator<Integer> iterator1 = mock(CloseableIterator.class);
        final CloseableIterator<Integer> iterator2 = mock(CloseableIterator.class);
        given(iterable1.iterator()).willReturn(iterator1);
        given(iterable2.iterator()).willReturn(iterator2);
        given(supplier.get()).willReturn(iterable1, iterable2);

        final SuppliedIterable<Integer> suppliedIter = new SuppliedIterable<>(supplier);
        suppliedIter.iterator();
        suppliedIter.iterator();

        // When 1
        suppliedIter.close();

        // Then 2
        verify(iterable1).close();
        verify(iterable2).close();
    }
}
