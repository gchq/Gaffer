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

import com.google.common.collect.Lists;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ChainedIterableTest {

    @Test
    public void shouldWrapAllIterables() {
        // Given
        final List<Integer> itr1 = Collections.singletonList(0);
        final List<Integer> itr2 = new ArrayList<>(0);
        final List<Integer> itr3 = Lists.newArrayList(1, 2, 3, 4);
        final List<Integer> itr4 = Lists.newArrayList(5, 6);

        // When
        final Iterable<Integer> wrappedItr = new ChainedIterable<>(itr1, itr2, itr3, itr4);

        // Then
        assertEquals(Lists.newArrayList(0, 1, 2, 3, 4, 5, 6), Lists.newArrayList(wrappedItr));
    }

    @Test
    public void shouldRemoveElementFromFirstIterable() {
        // Given
        final List<Integer> itr1 = Lists.newArrayList(0);
        final List<Integer> itr2 = new ArrayList<>(0);
        final List<Integer> itr3 = Lists.newArrayList(1, 2, 3, 4);
        final List<Integer> itr4 = Lists.newArrayList(5, 6);

        final int itr1Size = itr1.size();
        final int itr2Size = itr2.size();
        final int itr3Size = itr3.size();
        final int itr4Size = itr4.size();

        final Iterable<Integer> wrappedItr = new ChainedIterable<>(itr1, itr2, itr3, itr4);

        // When
        final Iterator<Integer> itr = wrappedItr.iterator();
        assertEquals(0, (int) itr.next());
        itr.remove();

        // Then
        assertEquals(itr1Size - 1, itr1.size());
        assertEquals(itr2Size, itr2.size());
        assertEquals(itr3Size, itr3.size());
        assertEquals(itr4Size, itr4.size());
    }

    @Test
    public void shouldRemoveElementFromThirdIterable() {
        // Given
        final List<Integer> itr1 = Lists.newArrayList(0);
        final List<Integer> itr2 = new ArrayList<>(0);
        final List<Integer> itr3 = Lists.newArrayList(1, 2, 3, 4);
        final List<Integer> itr4 = Lists.newArrayList(5, 6);

        final int itr1Size = itr1.size();
        final int itr2Size = itr2.size();
        final int itr3Size = itr3.size();
        final int itr4Size = itr4.size();

        final Iterable<Integer> wrappedItr = new ChainedIterable<>(itr1, itr2, itr3, itr4);

        // When
        final Iterator<Integer> itr = wrappedItr.iterator();
        assertEquals(0, (int) itr.next());
        assertEquals(1, (int) itr.next());
        itr.remove();

        // Then
        assertEquals(itr1Size, itr1.size());
        assertEquals(itr2Size, itr2.size());
        assertEquals(itr3Size - 1, itr3.size());
        assertEquals(itr4Size, itr4.size());
    }
}
