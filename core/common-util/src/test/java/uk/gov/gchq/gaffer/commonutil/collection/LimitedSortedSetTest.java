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

import org.junit.Test;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

public class LimitedSortedSetTest {

    @Test
    public void shouldLimitEntries() {
        // Given
        final LimitedSortedSet<Integer> multiSet = new LimitedSortedSet<Integer>(Comparator
                .naturalOrder(), 100);

        // When
        IntStream.rangeClosed(1, 200).forEach(multiSet::add);

        // Then
        assertThat(multiSet, hasSize(100));
    }

    @Test
    public void shouldSortEntries() {
        // Given
        final LimitedSortedSet<Integer> multiSet = new LimitedSortedSet<Integer>(Comparator
                .naturalOrder(), 100);

        // When
        IntStream.rangeClosed(1, 200).forEach(multiSet::add);

        // Then
        assertThat(multiSet.first(), equalTo(1));
        assertThat(multiSet.last(), equalTo(100));
    }

    @Test
    public void shouldAddAll() {
        // Given
        final LimitedSortedSet<Integer> multiSet = new LimitedSortedSet<Integer>(Comparator
                .naturalOrder(), 100);

        // When/Then
        final List<Integer> evens = IntStream.iterate(0, i -> i + 2)
                                             .limit(10)
                                             .boxed()
                                             .collect(Collectors.toList());
        
        final boolean evensResult = multiSet.addAll(evens);

        assertThat(evens, hasSize(10));
        assertThat(evensResult, equalTo(true));
        assertThat(multiSet, hasSize(10));
        assertThat(multiSet.first(), equalTo(0));
        assertThat(multiSet.last(), equalTo(18));

        final List<Integer> odds = IntStream.iterate(1, i -> i + 2)
                                            .limit(10)
                                            .boxed()
                                            .collect(Collectors.toList());

        final boolean oddsResult = multiSet.addAll(odds);

        assertThat(odds, hasSize(10));
        assertThat(oddsResult, equalTo(true));
        assertThat(multiSet, hasSize(20));
        assertThat(multiSet.first(), equalTo(0));
        assertThat(multiSet.last(), equalTo(19));
    }

    @Test
    public void shouldLimitEntriesOnAddAll() {
        // Given
        final LimitedSortedSet<Integer> multiSet = new LimitedSortedSet<Integer>(Comparator
                .naturalOrder(), 10);

        // When/Then
        final List<Integer> evens = IntStream.iterate(0, i -> i + 2)
                                             .limit(100)
                                             .boxed()
                                             .collect(Collectors.toList());

        final boolean evensResult = multiSet.addAll(evens);

        assertThat(evens, hasSize(100));
        assertThat(evensResult, equalTo(true));
        assertThat(multiSet, hasSize(10));
        assertThat(multiSet.first(), equalTo(0));
        assertThat(multiSet.last(), equalTo(18));

        final List<Integer> odds = IntStream.iterate(1, i -> i + 2)
                                            .limit(100)
                                            .boxed()
                                            .collect(Collectors.toList());

        final boolean oddsResult = multiSet.addAll(odds);

        assertThat(odds, hasSize(100));
        assertThat(oddsResult, equalTo(true));
        assertThat(multiSet, hasSize(10));
        assertThat(multiSet.first(), equalTo(0));
        assertThat(multiSet.last(), equalTo(9));
    }



    @Test
    public void shouldClone() {
        // Given
        final LimitedSortedSet<Integer> multiSet = new LimitedSortedSet<Integer>(Comparator
                .naturalOrder(), 100);
        IntStream.rangeClosed(1, 200).forEach(multiSet::add);

        // When
        final LimitedSortedSet<Integer> clone = multiSet.clone();

        // Then
        assertThat(clone, equalTo(multiSet));
    }

}
