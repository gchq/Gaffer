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

package uk.gov.gchq.gaffer.mapstore.multimap;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MapOfSetsTest {
    @Test
    void shouldPutValueInExistingMapSet() {
        // Given
        final String key = "key1";
        final String value = "value1";
        final Map<String, Set<String>> map = new HashMap<>();
        final Set<String> set = mock(Set.class);
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map);

        map.put(key, set);
        given(set.add(value)).willReturn(true);

        // When
        final boolean putResult = mapOfSets.put(key, value);

        // Then
        assertThat(putResult).isTrue();
        verify(set).add(value);
    }

    @Test
    void shouldPutValueInMapWhenNullSetAndNullSetClass() {
        // Given
        final String key = "key1";
        final String value = "value1";
        final Map<String, Set<String>> map = new HashMap<>();
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map);

        // When
        final boolean putResult = mapOfSets.put(key, value);

        // Then
        assertThat(putResult).isTrue();
        assertThat(mapOfSets.get(key)).isEqualTo(Sets.newHashSet(value));
    }

    @Test
    void shouldPutValueInMapWhenNullSetAndLinkedHashSetClass() {
        // Given
        final String key = "key1";
        final String value = "value1";
        final Map<String, Set<String>> map = new HashMap<>();
        final MapOfSets<String, String> mapOfSets = new MapOfSets<>(map, LinkedHashSet.class);

        // When
        final boolean putResult = mapOfSets.put(key, value);

        // Then
        assertThat(putResult).isTrue();
        assertThat(map).containsEntry(key, Sets.newLinkedHashSet(Collections.singleton(value)));
    }

    @Test
    void shouldGetSetFromMap() {
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
        assertThat(result).isSameAs(set);
    }

    @Test
    void shouldClearMap() {
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
