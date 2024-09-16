/*
 * Copyright 2017-2024 Crown Copyright
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class SuppliedIterableTest {

    @Test
    void shouldRequestNewIterableFromSupplierWhenIteratorInvoked(@Mock final Supplier<Iterable<Integer>> supplier) {
        // Given
        final Iterable<Integer> iterable1 = Arrays.asList(1, 2, 3);
        final Iterable<Integer> iterable2 = Arrays.asList(4, 5, 6);

        when(supplier.get())
                .thenReturn(iterable1)
                .thenReturn(iterable2);

        // When 1
        SuppliedIterable<Integer> suppliedItr = null;
        Iterator<Integer> result1 = null;
        Iterator<Integer> result2 = null;
        try {
            suppliedItr = new SuppliedIterable<>(supplier);
            result1 = suppliedItr.iterator();
            result2 = suppliedItr.iterator();

            // Then 2
            verify(supplier, times(2)).get();
            assertThat(result1).toIterable().containsExactlyElementsOf(iterable1);
            assertThat(result2).toIterable().containsExactlyElementsOf(iterable2);
        } finally {
            CloseableUtil.close(result2, result1, suppliedItr);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCloseIterables(@Mock final Supplier<Iterable<Integer>> supplier) throws IOException {
        // Given
        final Iterable<Integer> iterable1 = mock(Iterable.class, withSettings().extraInterfaces(Closeable.class));
        final Iterable<Integer> iterable2 = mock(Iterable.class, withSettings().extraInterfaces(Closeable.class));
        final Iterator<Integer> iterator1 = mock(Iterator.class, withSettings().extraInterfaces(Closeable.class));
        final Iterator<Integer> iterator2 = mock(Iterator.class, withSettings().extraInterfaces(Closeable.class));

        when(iterable1.iterator()).thenReturn(iterator1);
        when(iterable2.iterator()).thenReturn(iterator2);
        when(supplier.get()).thenReturn(iterable1, iterable2);

        SuppliedIterable<Integer> suppliedItr = null;
        try {
            suppliedItr = new SuppliedIterable<>(supplier);
            suppliedItr.iterator();
            suppliedItr.iterator();

            // When 1
            suppliedItr.close();

            // Then 2
            verify((Closeable) iterable1).close();
            verify((Closeable) iterable2).close();
        } finally {
            CloseableUtil.close(suppliedItr);
        }
    }
}
