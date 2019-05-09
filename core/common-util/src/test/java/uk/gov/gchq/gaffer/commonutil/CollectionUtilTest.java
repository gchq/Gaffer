/*
 * Copyright 2016-2019 Crown Copyright
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
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectionUtilTest {
    @Test
    public void shouldReturnTreeSetWithProvidedItem() {
        // Given
        final String item = "test item";

        // When
        final TreeSet<String> treeSet = CollectionUtil.treeSet(item);

        // Then
        assertEquals(1, treeSet.size());
        assertTrue(treeSet.contains(item));
    }

    @Test
    public void shouldReturnTreeSetWithWithOutNullItem() {
        // Given
        final String item = null;

        // When
        final TreeSet<String> treeSet = CollectionUtil.treeSet(item);

        // Then
        assertEquals(0, treeSet.size());
    }

    @Test
    public void shouldReturnTreeSetWithProvidedItems() {
        // Given
        final String[] items = {"test item 1", "test item 2", null};

        // When
        final TreeSet<String> treeSet = CollectionUtil.treeSet(items);

        // Then
        assertEquals(2, treeSet.size());
        for (final String item : items) {
            if (null != item) {
                assertTrue(treeSet.contains(item));
            }
        }
    }

    @Test
    public void shouldReturnTreeSetWithNoItemsForNullArray() {
        // Given
        final String[] items = null;

        // When
        final TreeSet<String> treeSet = CollectionUtil.treeSet(items);

        // Then
        assertEquals(0, treeSet.size());
    }

    @Test
    public void shouldConvertMapToStringKeys() {
        // Given
        final Map<Class<? extends Number>, String> map = new HashMap<>();
        map.put(Integer.class, "integer");
        map.put(Double.class, "double");
        map.put(Long.class, "long");

        // When
        final Map<String, String> result = CollectionUtil.toMapWithStringKeys(map);

        // Then
        final Map<String, String> expectedResult = new HashMap<>();
        expectedResult.put(Integer.class.getName(), "integer");
        expectedResult.put(Double.class.getName(), "double");
        expectedResult.put(Long.class.getName(), "long");
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldConvertMapToStringKeysWithProvidedMap() {
        // Given
        final Map<Class<? extends Number>, String> map = new HashMap<>();
        map.put(Integer.class, "integer");
        map.put(Double.class, "double");
        map.put(Long.class, "long");

        final Map<String, String> result = new LinkedHashMap<>();

        // When
        CollectionUtil.toMapWithStringKeys(map, result);

        // Then
        final Map<String, String> expectedResult = new LinkedHashMap<>();
        expectedResult.put(Integer.class.getName(), "integer");
        expectedResult.put(Double.class.getName(), "double");
        expectedResult.put(Long.class.getName(), "long");
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldConvertMapToClassKeys() throws ClassNotFoundException {
        // Given
        final Map<String, String> map = new HashMap<>();
        map.put(Integer.class.getName(), "integer");
        map.put(Double.class.getName(), "double");
        map.put(Long.class.getName(), "long");

        // When
        final Map<Class<? extends Number>, String> result = CollectionUtil.toMapWithClassKeys(map);

        // Then
        final Map<Class<? extends Number>, String> expectedResult = new HashMap<>();
        expectedResult.put(Integer.class, "integer");
        expectedResult.put(Double.class, "double");
        expectedResult.put(Long.class, "long");
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldConvertMapToClassKeysWithProvidedMap() throws ClassNotFoundException {
        // Given
        final Map<String, String> map = new HashMap<>();
        map.put(Integer.class.getName(), "integer");
        map.put(Double.class.getName(), "double");
        map.put(Long.class.getName(), "long");

        final Map<Class<? extends Number>, String> result = new LinkedHashMap<>();


        // When
        CollectionUtil.toMapWithClassKeys(map, result);

        // Then
        final Map<Class<? extends Number>, String> expectedResult = new LinkedHashMap<>();
        expectedResult.put(Integer.class, "integer");
        expectedResult.put(Double.class, "double");
        expectedResult.put(Long.class, "long");
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldReturnTrueWhenCollectionContainsAProvidedValue() {
        // Given
        final Collection collection = Sets.newHashSet(10, 20, 2, 30);
        final Object[] values = new Object[]{1, 2, 3};

        // When
        final boolean result = CollectionUtil.containsAny(collection, values);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenCollectionDoesNotContainsAProvidedValue() {
        // Given
        final Collection collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = new Object[]{1, 2, 3};

        // When
        final boolean result = CollectionUtil.containsAny(collection, values);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldReturnFalseWhenContainsAnyCalledWithNullValue() {
        // Given
        final Collection collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = null;

        // When
        final boolean result = CollectionUtil.containsAny(collection, values);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldReturnFalseWhenContainsAnyCalledWithNullCollection() {
        // Given
        final Collection collection = null;
        final Object[] values = new Object[]{1, 2, 3};

        // When
        final boolean result = CollectionUtil.containsAny(collection, values);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldReturnFalseWhenAnyMissingCalledWhenTheCollectionContainsAllValues() {
        // Given
        final Collection collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = new Object[]{10, 20, 30};

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldReturnFalseWhenAnyMissingCalledWhenNullValues() {
        // Given
        final Collection collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = null;

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldReturnTrueWhenAnyMissingCalledWhenTheCollectionDoesNotContainAProvidedValue() {
        // Given
        final Collection collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = new Object[]{1, 2, 3};

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenAnyMissingCalledWithNullValue() {
        // Given
        final Collection collection = Sets.newHashSet(10, 20, 30);
        final Object[] values = null;

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldReturnTrueWhenAnyMissingCalledWithNullCollectionAndSomeValues() {
        // Given
        final Collection collection = null;
        final Object[] values = new Object[]{1, 2, 3};

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenAnyMissingCalledWithNullCollectionAndValues() {
        // Given
        final Collection collection = null;
        final Object[] values = null;

        // When
        final boolean result = CollectionUtil.anyMissing(collection, values);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldReturnTrueWhenDistinctCalledWithCollectionOfUniqueValues() {
        // Given
        final Collection collection = Lists.newArrayList(1, 2, 3, 4, 5);

        // When
        final boolean result = CollectionUtil.distinct(collection);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenDistinctCalledWithCollectionOfNonUniqueValues() {
        // Given
        final Collection collection = Lists.newArrayList(1, 2, 3, 1, 2);

        // When
        final boolean result = CollectionUtil.distinct(collection);

        // Then
        assertFalse(result);
    }
}
