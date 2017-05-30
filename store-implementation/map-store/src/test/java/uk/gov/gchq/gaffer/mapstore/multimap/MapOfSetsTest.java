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
package uk.gov.gchq.gaffer.mapstore.multimap;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.gchq.gaffer.store.StoreException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MapOfSetsTest {
    @Test
    public void shouldPutValueInExistingMapSet() throws StoreException {
        // Given
        final String key = "key1";
        final String value = "value1";
        final Map<String, Set<String>> map = mock(Map.class);
        final Set<String> set = mock(Set.class);
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map);

        given(map.get(key)).willReturn(set);
        given(set.add(value)).willReturn(true);

        // When
        final boolean putResult = mapOfSets.put(key, value);

        // Then
        assertTrue(putResult);
        verify(map, never()).put(any(String.class), any(Set.class));
        verify(set).add(value);
    }

    @Test
    public void shouldPutValueInMapWhenNullSetAndNullSetClass() throws StoreException {
        // Given
        final String key = "key1";
        final String value = "value1";
        final Map<String, Set<String>> map = mock(Map.class);
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map);

        given(map.get(key)).willReturn(null);

        // When
        final boolean putResult = mapOfSets.put(key, value);

        // Then
        assertTrue(putResult);
        final ArgumentCaptor<Set> setCaptor = ArgumentCaptor.forClass(Set.class);
        verify(map).put(eq(key), setCaptor.capture());
        assertEquals(Sets.newHashSet(value), setCaptor.getValue());
    }

    @Test
    public void shouldPutValueInMapWhenNullSetAndLinkedHashSetClass() throws StoreException {
        // Given
        final String key = "key1";
        final String value = "value1";
        final Map<String, Set<String>> map = mock(Map.class);
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map, LinkedHashSet.class);

        given(map.get(key)).willReturn(null);

        // When
        final boolean putResult = mapOfSets.put(key, value);

        // Then
        assertTrue(putResult);
        final ArgumentCaptor<Set> setCaptor = ArgumentCaptor.forClass(Set.class);
        verify(map).put(eq(key), setCaptor.capture());
        assertEquals(Sets.newLinkedHashSet(Collections.singleton(value)), setCaptor.getValue());
    }

    @Test
    public void shouldGetSetFromMap() throws StoreException {
        // Given
        final String key = "key1";
        final Set<String> set = mock(Set.class);
        final Map<String, Set<String>> map = mock(Map.class);
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map, LinkedHashSet.class);

        given(map.get(key)).willReturn(set);

        // When
        final Collection<String> result = mapOfSets.get(key);

        // Then
        verify(map).get(key);
        assertSame(set, result);
    }

    @Test
    public void shouldClearMap() throws StoreException {
        // Given
        final String key = "key1";
        final Set<String> set = mock(Set.class);
        final Map<String, Set<String>> map = mock(Map.class);
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map, LinkedHashSet.class);

        given(map.get(key)).willReturn(set);

        // When
        mapOfSets.clear();

        // Then
        verify(map).clear();
    }
}
