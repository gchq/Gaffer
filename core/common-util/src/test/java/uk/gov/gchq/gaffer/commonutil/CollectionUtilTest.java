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

package uk.gov.gchq.gaffer.commonutil;

import org.junit.Test;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
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
}
