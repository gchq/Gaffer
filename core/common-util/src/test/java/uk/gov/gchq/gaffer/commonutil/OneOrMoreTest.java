/*
 * Copyright 2017-2020 Crown Copyright
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OneOrMoreTest {

    @Test
    public void shouldDeduplicateByDefault() {
        final OneOrMore<Integer> collection = new OneOrMore<>();

        collection.add(1);
        collection.add(1);

        assertEquals(1, collection.size());
        assertEquals(1, (int) collection.iterator().next());
    }

    @Test
    public void shouldAddItemInConstructor() {
        final boolean deduplicate = false;

        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate, 1);

        assertEquals(1, collection.size());
        assertEquals(1, (int) collection.iterator().next());
    }

    @Test
    public void shouldRemoveAnyItem() {
        final boolean deduplicate = true;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate, 1);

        collection.removeAnyItem();

        assertTrue(collection.isEmpty());
        assertEquals(0, collection.size());
    }

    @Test
    public void shouldRemoveLastItemInList() {
        final boolean deduplicate = false;
        final OneOrMore<Integer> collection = new OneOrMore<>(deduplicate, 1);
        collection.add(2);
        collection.add(3);

        collection.removeAnyItem();

        assertEquals(2, collection.size());
        assertEquals(Arrays.asList(1, 2), Lists.newArrayList(collection));
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
        assertEquals(400, collection.size());
        assertEquals(expectedItems, Sets.newHashSet(collection));
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
        assertEquals(expectedItems, Sets.newHashSet(collection));
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
        assertEquals(expectedItems.size() * 2, collection.size());
        assertEquals(expectedItems, Sets.newHashSet(collection));
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
        assertEquals(expectedItems, Sets.newHashSet(collection));
    }

    @Test
    public void shouldGetEmptyIterator() {
        // Given
        final OneOrMore<Integer> collection = new OneOrMore<>();

        // When
        final Iterator<Integer> itr = collection.iterator();

        // Then
        assertFalse(itr.hasNext());
    }

    @Test
    public void shouldGetSingletonIterator() {
        // Given
        final OneOrMore<Integer> collection = new OneOrMore<>();
        collection.add(1);

        // When
        final Iterator<Integer> itr = collection.iterator();

        // Then
        assertTrue(itr.hasNext());
        assertEquals(1, (int) itr.next());
        assertFalse(itr.hasNext());
    }

    @Test
    public void shouldBeEqual() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 1);

        assertEquals(collection1, collection2);
        assertEquals(collection1.hashCode(), collection2.hashCode());
    }

    @Test
    public void shouldBeEqualWithDeduplicate() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(true, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(true, 1);

        assertEquals(collection1, collection2);
        assertEquals(collection1.hashCode(), collection2.hashCode());
    }

    @Test
    public void shouldBeEqualWithMultipleValues() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        collection1.add(2);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 1);
        collection2.add(2);

        assertEquals(collection1, collection2);
        assertEquals(collection1.hashCode(), collection2.hashCode());
    }

    @Test
    public void shouldBeEqualWithMultipleValuesWithDeduplicate() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(true, 1);
        collection1.add(2);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(true, 1);
        collection2.add(2);

        assertEquals(collection1, collection2);
        assertEquals(collection1.hashCode(), collection2.hashCode());
    }

    @Test
    public void shouldNotBeEqual() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 2);

        assertNotEquals(collection1, collection2);
        assertNotEquals(collection1.hashCode(), collection2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWhenDeduplicateDifferent() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(true, 1);

        assertNotEquals(collection1, collection2);
        assertNotEquals(collection1.hashCode(), collection2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWithMultipleValues() {
        final OneOrMore<Integer> collection1 = new OneOrMore<>(false, 1);
        collection1.add(2);
        final OneOrMore<Integer> collection2 = new OneOrMore<>(false, 1);
        collection2.add(3);

        assertNotEquals(collection1, collection2);
        assertNotEquals(collection1.hashCode(), collection2.hashCode());
    }
}
