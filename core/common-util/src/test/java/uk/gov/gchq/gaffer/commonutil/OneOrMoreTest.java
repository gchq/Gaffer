/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class OneOrMoreTest {

    @Test
    public void shouldDeduplicateByDefault() {
        final OneOrMore<Integer> collection = new OneOrMore<>();

        collection.add(1);
        collection.add(1);

        assertThat(collection).hasSize(1);
        assertThat((int) collection.iterator().next()).isEqualTo(1);
    }

    @Test
    public void shouldAddItemInConstructor() {
        final boolean deduplicate = false;

        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate, 1);

        assertThat(collection).hasSize(1);
        assertThat((int) collection.iterator().next()).isEqualTo(1);
    }

    @Test
    public void shouldRemoveAnyItem() {
        final boolean deduplicate = true;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate, 1);

        collection.removeAnyItem();

        assertThat(collection).isEmpty();
    }

    @Test
    public void shouldRemoveLastItemInList() {
        final boolean deduplicate = false;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate, 1);
        collection.add(2);
        collection.add(3);

        collection.removeAnyItem();

        assertThat(collection)
                .hasSize(2)
                .containsExactly(1, 2);
    }

    @Test
    public void shouldAddItemsWithoutDeduplicate() {
        // Given
        final boolean deduplicate = false;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate);

        final Set<Integer> expectedItems = new HashSet<>();
        IntStream.rangeClosed(1, 200).forEach(expectedItems::add);

        // When
        for (int i = 200; 0 < i; i--) {
            collection.add(i);
            collection.add(i);
        }

        // Then
        assertThat(collection)
                .hasSize(400)
                .containsAll(expectedItems);
    }

    @Test
    public void shouldAddItemsWithDeduplicate() {
        // Given
        final boolean deduplicate = true;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate);
        final Set<Integer> expectedItems = new HashSet<>();
        IntStream.rangeClosed(1, 200).forEach(expectedItems::add);

        // When
        for (int i = 200; 0 < i; i--) {
            collection.add(i);
            collection.add(i);
        }

        // Then
        assertThat(collection).containsExactlyElementsOf(expectedItems);
    }

    @Test
    public void shouldAddAllItemsWithoutDeduplicate() {
        // Given
        final boolean deduplicate = false;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate);

        final Set<Integer> expectedItems = new HashSet<>();
        IntStream.rangeClosed(1, 200).forEach(expectedItems::add);

        // When
        collection.addAll(expectedItems);
        collection.addAll(expectedItems);

        // Then
        assertThat(collection)
                .hasSize(expectedItems.size() * 2)
                .containsAll(expectedItems);
    }

    @Test
    public void shouldAddAllItemsWithDeduplicate() {
        // Given
        final boolean deduplicate = true;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate);

        final Set<Integer> expectedItems = new HashSet<>();
        IntStream.rangeClosed(1, 200).forEach(expectedItems::add);

        // When
        collection.addAll(expectedItems);
        collection.addAll(expectedItems);

        // Then
        assertThat(collection).containsExactlyElementsOf(expectedItems);
    }

    @Test
    public void shouldGetEmptyIterator() {
        // Given
        final OneOrMore<Integer> collection = new OneOrMore<>();

        // When
        final Iterator<Integer> itr = collection.iterator();

        // Then
        assertThat(itr).isExhausted();
    }

    @Test
    public void shouldGetSingletonIterator() {
        // Given
        final OneOrMore<Integer> collection = new OneOrMore<>();
        collection.add(1);

        // When
        final Iterator<Integer> itr = collection.iterator();

        // Then
        assertThat(itr).hasNext();
        assertThat(itr.next()).isEqualTo(1);
        assertThat(itr).isExhausted();
    }

    @Test
    public void shouldBeEqual() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 1);

        assertThat(collection1)
                .isEqualTo(collection2)
                .hasSameHashCodeAs(collection2);
    }

    @Test
    public void shouldBeEqualWithDeduplicate() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(true, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(true, 1);

        assertThat(collection1)
                .isEqualTo(collection2)
                .hasSameHashCodeAs(collection2);
    }

    @Test
    public void shouldBeEqualWithMultipleValues() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        collection1.add(2);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 1);
        collection2.add(2);

        assertThat(collection1)
                .isEqualTo(collection2)
                .hasSameHashCodeAs(collection2);
    }

    @Test
    public void shouldBeEqualWithMultipleValuesWithDeduplicate() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(true, 1);
        collection1.add(2);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(true, 1);
        collection2.add(2);

        assertThat(collection1)
                .isEqualTo(collection2)
                .hasSameHashCodeAs(collection2);
    }

    @Test
    public void shouldNotBeEqual() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 2);

        assertThat(collection1)
                .isNotEqualTo(collection2)
                .doesNotHaveSameHashCodeAs(collection2);
    }

    @Test
    public void shouldNotBeEqualWhenDeduplicateDifferent() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(true, 1);

        assertThat(collection1)
                .isNotEqualTo(collection2)
                .doesNotHaveSameHashCodeAs(collection2);
    }

    @Test
    public void shouldNotBeEqualWithMultipleValues() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        collection1.add(2);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 1);
        collection2.add(3);

        assertThat(collection1)
                .isNotEqualTo(collection2)
                .doesNotHaveSameHashCodeAs(collection2);
    }
}
