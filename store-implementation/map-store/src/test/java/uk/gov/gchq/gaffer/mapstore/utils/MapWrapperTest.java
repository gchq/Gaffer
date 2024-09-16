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

package uk.gov.gchq.gaffer.mapstore.utils;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MapWrapperTest {

    @Test
    void shouldDelegateAllCallsToMap() {
        // Given
        final String key = "key1";
        final String value = "value1";
        final String value2 = "value2";
        final int size = 10;
        final boolean isEmpty = false;
        final boolean containsKey = true;
        final boolean containsValue = true;
        final Map<String, String> map = mock(Map.class);
        final Map<String, String> map2 = mock(Map.class);
        final Set<String> keySet = mock(Set.class);
        final Collection<String> values = mock(Collection.class);
        final Set<Map.Entry<String, String>> entrySet = mock(Set.class);
        given(map.put(key, value)).willReturn(value2);
        given(map.get(key)).willReturn(value);
        given(map.size()).willReturn(size);
        given(map.isEmpty()).willReturn(isEmpty);
        given(map.containsKey(key)).willReturn(containsKey);
        given(map.containsValue(value)).willReturn(containsValue);
        given(map.remove(key)).willReturn(value);
        given(map.keySet()).willReturn(keySet);
        given(map.values()).willReturn(values);
        given(map.entrySet()).willReturn(entrySet);

        final MapWrapper<String, String> wrapper = new MapWrapper<>(map);

        // When / Then - put
        final String putResult = wrapper.put(key, value);
        verify(map).put(key, value);
        assertThat(putResult).isEqualTo(value2);

        // When / Then - get
        final String getResult = wrapper.get(key);
        verify(map).get(key);
        assertThat(getResult).isEqualTo(value);

        // When / Then - size
        final int sizeResult = wrapper.size();
        verify(map).size();
        assertThat(sizeResult).isEqualTo(size);

        // When / Then - isEmpty
        final boolean isEmptyResult = wrapper.isEmpty();
        verify(map).size();
        assertThat(isEmptyResult).isEqualTo(isEmpty);

        // When / Then - containsKey
        final boolean containsKeyResult = wrapper.containsKey(key);
        verify(map).containsKey(key);
        assertThat(containsKeyResult).isEqualTo(containsKey);

        // When / Then - containsValue
        final boolean containsValueResult = wrapper.containsValue(value);
        verify(map).containsValue(value);
        assertThat(containsValueResult).isEqualTo(containsValue);

        // When / Then - remove
        final String removeResult = wrapper.remove(key);
        verify(map).remove(key);
        assertThat(removeResult).isEqualTo(value);

        // When / Then - putAll
        wrapper.putAll(map2);
        verify(map).putAll(map2);

        // When / Then - clear
        wrapper.clear();
        verify(map).clear();

        // When / Then - keySet
        final Set<String> keySetResult = wrapper.keySet();
        verify(map).keySet();
        assertThat(keySetResult).isEqualTo(keySet);

        // When / Then - values
        final Collection<String> valuesResult = wrapper.values();
        verify(map).values();
        assertThat(valuesResult).isSameAs(values);

        // When / Then - entrySet
        final Set<Map.Entry<String, String>> entrySetResult = wrapper.entrySet();
        verify(map).entrySet();
        assertThat(entrySetResult).isSameAs(entrySet);

        // When / Then - getMap
        final Map<String, String> mapResult = wrapper.getMap();
        assertThat(mapResult).isSameAs(map);
    }
}
