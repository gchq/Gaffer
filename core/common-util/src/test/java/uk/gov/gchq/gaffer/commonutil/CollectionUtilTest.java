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

package uk.gov.gchq.gaffer.commonutil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class CollectionUtilTest {

    @Test
    void shouldReturnTreeSetWithProvidedItem() {
        final String item = "test item";

        final TreeSet<String> treeSet = CollectionUtil.treeSet(item);

        assertThat(treeSet)
                .containsExactly(item);
    }

    @Test
    void shouldReturnTreeSetWithWithOutNullItem() {
        final TreeSet<String> treeSet = CollectionUtil.treeSet((String) null);

        assertThat(treeSet).isEmpty();
    }

    @Test
     void shouldReturnTreeSetWithProvidedItems() {
        final String[] items = {"test item 1", "test item 2", null};

        final TreeSet<String> treeSet = CollectionUtil.treeSet(items);

        assertThat(treeSet).hasSize(2);
        for (final String item : items) {
            if (null != item) {
                assertThat(treeSet).contains(item);
            }
        }
    }

    @Test
    void shouldConvertMapToStringKeys() {
        // Given
        final Map<Class<? extends Number>, String> map = new HashMap<>();
        populateClassKeyMap(map);

        // When
        final Map<String, String> result = CollectionUtil.toMapWithStringKeys(map);

        // Then
        final Map<String, String> expectedResult = new HashMap<>();
        populateStringKeyMap(expectedResult);
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void shouldConvertMapToStringKeysWithProvidedMap() {
        // Given
        final Map<Class<? extends Number>, String> map = new HashMap<>();
        populateClassKeyMap(map);

        final Map<String, String> result = new LinkedHashMap<>();

        // When
        CollectionUtil.toMapWithStringKeys(map, result);

        // Then
        final Map<String, String> expectedResult = new LinkedHashMap<>();
        populateStringKeyMap(expectedResult);
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void shouldConvertMapToClassKeys() throws ClassNotFoundException {
        // Given
        final Map<String, String> map = new HashMap<>();
        populateStringKeyMap(map);

        // When
        final Map<Class<? extends Number>, String> result = CollectionUtil.toMapWithClassKeys(map);

        // Then
        final Map<Class<? extends Number>, String> expectedResult = new HashMap<>();
        populateClassKeyMap(expectedResult);
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void shouldConvertMapToClassKeysWithProvidedMap() throws ClassNotFoundException {
        // Given
        final Map<String, String> map = new HashMap<>();
        populateStringKeyMap(map);

        final Map<Class<? extends Number>, String> result = new LinkedHashMap<>();

        // When
        CollectionUtil.toMapWithClassKeys(map, result);

        // Then
        final Map<Class<? extends Number>, String> expectedResult = new LinkedHashMap<>();
        populateClassKeyMap(expectedResult);
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void shouldReturnTrueWhenCollectionContainsAProvidedValue() {
        // Given
        final Collection<Integer> collection = Sets.newHashSet(10, 20, 2, 30);
        final Object[] values = new Object[] {1, 2, 3};

        // When
        final boolean result = CollectionUtil.containsAny(collection, values);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldReturnFalseWhenCollectionDoesNotContainsAProvidedValue() {
        // Given
        final Collection<Integer> collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = new Object[] {1, 2, 3};

        // When
        final boolean result = CollectionUtil.containsAny(collection, values);

        // Then
        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnFalseWhenContainsAnyCalledWithNullValue() {
        // Given
        final Collection<Integer> collection = Sets.newHashSet(10, 20, 30);

        // When
        final boolean result = CollectionUtil.containsAny(collection, null);

        // Then
        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnFalseWhenContainsAnyCalledWithNullCollection() {
        final Object[] values = new Object[] {1, 2, 3};

        final boolean result = CollectionUtil.containsAny(null, values);

        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnFalseWhenAnyMissingCalledWhenTheCollectionContainsAllValues() {
        // Given
        final Collection<Integer> collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = new Object[] {10, 20, 30};

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnFalseWhenAnyMissingCalledWhenNullValues() {
        // Given
        final Collection<Integer> collection = Sets.newHashSet(10, 20, 30);

        // When
        final boolean result = CollectionUtil.anyMissing(collection, null);

        // Then
        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnTrueWhenAnyMissingCalledWhenTheCollectionDoesNotContainAProvidedValue() {
        // Given
        final Collection<Integer> collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = new Object[] {1, 2, 3};

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldReturnTrueWhenAnyMissingCalledWithNullCollectionAndSomeValues() {
        // Given
        final Object[] values = new Object[] {1, 2, 3};

        // When
        final boolean result = CollectionUtil.anyMissing(null, values);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldReturnFalseWhenAnyMissingCalledWithNullCollectionAndValues() {
        // When
        final boolean result = CollectionUtil.anyMissing(null, null);

        // Then
        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnTrueWhenDistinctCalledWithCollectionOfUniqueValues() {
        // Given
        final Collection<Integer> collection = Lists.newArrayList(1, 2, 3, 4, 5);

        // When
        final boolean result = CollectionUtil.distinct(collection);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldReturnFalseWhenDistinctCalledWithCollectionOfNonUniqueValues() {
        // Given
        final Collection<Integer> collection = Lists.newArrayList(1, 2, 3, 1, 2);

        // When
        final boolean result = CollectionUtil.distinct(collection);

        // Then
        assertThat(result).isFalse();
    }

    private void populateClassKeyMap(Map<Class<? extends Number>, String> map) {
        map.put(Integer.class, "integer");
        map.put(Double.class, "double");
        map.put(Long.class, "long");
    }

    private void populateStringKeyMap(Map<String, String> map) {
        map.put(Integer.class.getName(), "integer");
        map.put(Double.class.getName(), "double");
        map.put(Long.class.getName(), "long");
    }
}
