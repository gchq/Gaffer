/*
 * Copyright 2016-2020 Crown Copyright
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChainedIterableTest {

    @Test
    public void shouldThrowNSEXWhenNoNextIterableWhenOneElementAndNo2ndNext() {
        final Iterable<Integer> chainedIterable = new ChainedIterable<>(Collections.singletonList(1));
        final Iterator<Integer> iterator = chainedIterable.iterator();

        iterator.next();

        // No 2nd element
        assertThrows(NoSuchElementException.class, () -> iterator.next());
    }

    @Test
    public void shouldThrowIAXWhenIterablesAreEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new ChainedIterable<>());
    }

    @Test
    public void shouldThrowIAXWhenIterablesAreNull() {
        assertThrows(IllegalArgumentException.class, () -> new ChainedIterable<>(null));
    }

    @Test
    public void shouldWrapAllIterables() {
        final List<Integer> itr1 = Collections.singletonList(0);
        final List<Integer> emptyItr2 = new ArrayList<>(0);
        final List<Integer> itr3 = Lists.newArrayList(1, 2, 3, 4);
        final List<Integer> itr4 = Lists.newArrayList(5, 6);

        final Iterable<Integer> wrappedItr = new ChainedIterable<>(itr1, emptyItr2, itr3, itr4);

        assertEquals(Lists.newArrayList(0, 1, 2, 3, 4, 5, 6), Lists.newArrayList(wrappedItr));
    }

    @Test
    public void shouldRemoveElementFromFirstIterable() {
        // Given
        final List<String> itr1 = Lists.newArrayList("a");
        final List<String> emptyItr2 = new ArrayList<>(0);
        final List<String> itr3 = Lists.newArrayList("b", "c", "d", "e");
        final List<String> itr4 = Lists.newArrayList("f", "g");

        final Iterable<String> wrappedItr = new ChainedIterable<>(itr1, emptyItr2, itr3, itr4);

        // When
        final Iterator<String> itr = wrappedItr.iterator();
        assertEquals("a", itr.next());

        itr.remove();

        // Then
        assertEquals(0, itr1.size());
        assertEquals(0, emptyItr2.size());
        assertEquals(4, itr3.size());
        assertEquals(2, itr4.size());
    }

    @Test
    public void shouldRemoveElementFromThirdIterable() {
        // Given
        final List<String> itr1 = Lists.newArrayList("a");
        final List<String> emptyItr2 = new ArrayList<>(0);
        final List<String> itr3 = Lists.newArrayList("b", "c", "d", "e");
        final List<String> itr4 = Lists.newArrayList("f", "g");

        final Iterable<String> wrappedItr = new ChainedIterable<>(itr1, emptyItr2, itr3, itr4);

        // When
        final Iterator<String> itr = wrappedItr.iterator();
        assertEquals("a", itr.next());
        assertEquals("b", itr.next());

        itr.remove();

        // Then
        assertEquals(1, itr1.size());
        assertEquals(0, emptyItr2.size());
        assertEquals(3, itr3.size());
        assertEquals(2, itr4.size());
    }
}
