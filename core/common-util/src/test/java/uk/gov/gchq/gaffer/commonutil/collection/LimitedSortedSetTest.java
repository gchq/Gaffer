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
package uk.gov.gchq.gaffer.commonutil.collection;

import com.google.common.collect.Lists;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;

public class LimitedSortedSetTest {

    @Test
    public void shouldLimitEntries() {
        // Given
        final LimitedSortedSet<Integer> set = new LimitedSortedSet<Integer>(Comparator.naturalOrder(), 100);
        final List<Integer> expectedItems = new ArrayList<>();
        IntStream.rangeClosed(1, 100).forEach(expectedItems::add);

        // When
        for (int i = 200; i > 0; i--) {
            set.add(i);
        }

        // Then
        assertEquals(expectedItems, Lists.newArrayList(set));
    }

    @Test
    public void shouldAddAll() {
        // Given
        final LimitedSortedSet<Integer> set = new LimitedSortedSet<Integer>(Comparator
                .naturalOrder(), 100);

        // When/Then
        final List<Integer> evens = IntStream.iterate(0, i -> i + 2)
                .limit(10)
                .boxed()
                .collect(Collectors.toList());

        final boolean evensResult = set.addAll(evens);

        assertThat(evens, hasSize(10));
        assertThat(evensResult, equalTo(true));
        assertThat(set, hasSize(10));
        assertThat(set.first(), equalTo(0));
        assertThat(set.last(), equalTo(18));

        final List<Integer> odds = IntStream.iterate(1, i -> i + 2)
                .limit(10)
                .boxed()
                .collect(Collectors.toList());

        final boolean oddsResult = set.addAll(odds);

        assertThat(odds, hasSize(10));
        assertThat(oddsResult, equalTo(true));
        assertThat(set, hasSize(20));
        assertThat(set.first(), equalTo(0));
        assertThat(set.last(), equalTo(19));
    }

    @Test
    public void shouldLimitEntriesOnAddAll() {
        // Given
        final LimitedSortedSet<Integer> set = new LimitedSortedSet<Integer>(Comparator
                .naturalOrder(), 10);

        // When/Then
        final List<Integer> evens = IntStream.iterate(0, i -> i + 2)
                .limit(100)
                .boxed()
                .collect(Collectors.toList());

        final boolean evensResult = set.addAll(evens);

        assertThat(evens, hasSize(100));
        assertThat(evensResult, equalTo(true));
        assertThat(set, hasSize(10));
        assertThat(set.first(), equalTo(0));
        assertThat(set.last(), equalTo(18));

        final List<Integer> odds = IntStream.iterate(1, i -> i + 2)
                .limit(100)
                .boxed()
                .collect(Collectors.toList());

        final boolean oddsResult = set.addAll(odds);

        assertThat(odds, hasSize(100));
        assertThat(oddsResult, equalTo(true));
        assertThat(set, hasSize(10));
        assertThat(set.first(), equalTo(0));
        assertThat(set.last(), equalTo(9));
    }
}
