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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class ChainedIterableTest {

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNSEXWhenNoNextIterableWhenOneElementAndNo2ndNext() {
        ChainedIterable<Integer> chainedIterable = null;
        try {
            chainedIterable = new ChainedIterable<>(Collections.singletonList(1));
            final Iterator<Integer> iterator = chainedIterable.iterator();

            assertThat(iterator.next()).isEqualTo(1);
            // No 2nd element
            assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> iterator.next());
        } finally {
            CloseableUtil.close(chainedIterable);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowIAXWhenArrayOfIterablesAreEmpty() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ChainedIterable<>());
    }

    @Test
    public void shouldThrowIAXWhenArrayOfIterablesAreNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ChainedIterable<>((Iterable<Integer>[]) null));
    }

    @Test
    public void shouldWrapAllIterableOfIterables() {
        // Given
        final List<Integer> itr1 = Collections.singletonList(0);
        final List<Integer> emptyItr2 = new ArrayList<>(0);
        final List<Integer> itr3 = Lists.newArrayList(1, 2, 3, 4);
        final List<Integer> itr4 = Lists.newArrayList(5, 6);

        // When
        final List<List<Integer>> collect = Stream.of(itr1, emptyItr2, itr3, itr4).collect(Collectors.toList());

        ChainedIterable<Integer> wrappedItr = null;
        try {
            wrappedItr = new ChainedIterable<>(collect);

            // Then
            assertThat(wrappedItr).containsExactly(0, 1, 2, 3, 4, 5, 6);
        } finally {
            CloseableUtil.close(wrappedItr);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapAllArrayOfIterables() {
        // Given
        final List<Integer> itr1 = Collections.singletonList(0);
        final List<Integer> emptyItr2 = new ArrayList<>(0);
        final List<Integer> itr3 = Lists.newArrayList(1, 2, 3, 4);
        final List<Integer> itr4 = Lists.newArrayList(5, 6);

        // When
        ChainedIterable<Integer> wrappedItr = null;
        try {
            wrappedItr = new ChainedIterable<>(itr1, emptyItr2, itr3, itr4);
            // Then
            assertThat(wrappedItr).containsExactly(0, 1, 2, 3, 4, 5, 6);
        } finally {
            CloseableUtil.close(wrappedItr);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldRemoveElementFromFirstIterable() {
        // Given
        final List<String> itr1 = Lists.newArrayList("a");
        final List<String> emptyItr2 = new ArrayList<>(0);
        final List<String> itr3 = Lists.newArrayList("b", "c", "d", "e");
        final List<String> itr4 = Lists.newArrayList("f", "g");

        ChainedIterable<String> wrappedItr = null;
        Iterator<String> itr = null;
        try {
            wrappedItr = new ChainedIterable<>(itr1, emptyItr2, itr3, itr4);

            // When
            itr = wrappedItr.iterator();
            assertThat(itr.next()).isEqualTo("a");

            itr.remove();

            // Then
            assertThat(itr1).isEmpty();
            assertThat(emptyItr2).isEmpty();
            assertThat(itr3).hasSize(4);
            assertThat(itr4).hasSize(2);
        } finally {
            CloseableUtil.close(itr);
            CloseableUtil.close(wrappedItr);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldRemoveElementFromThirdIterable() {
        // Given
        final List<String> itr1 = Lists.newArrayList("a");
        final List<String> emptyItr2 = new ArrayList<>(0);
        final List<String> itr3 = Lists.newArrayList("b", "c", "d", "e");
        final List<String> itr4 = Lists.newArrayList("f", "g");

        // When
        ChainedIterable<String> wrappedItr = null;
        Iterator<String> itr = null;
        try {
            wrappedItr = new ChainedIterable<>(itr1, emptyItr2, itr3, itr4);

            // When
            itr = wrappedItr.iterator();
            assertThat(itr.next()).isEqualTo("a");
            assertThat(itr.next()).isEqualTo("b");

            itr.remove();

            // Then
            assertThat(itr1).hasSize(1);
            assertThat(emptyItr2).isEmpty();
            assertThat(itr3).hasSize(3);
            assertThat(itr4).hasSize(2);
        } finally {
            CloseableUtil.close(itr);
            CloseableUtil.close(wrappedItr);
        }
    }
}
