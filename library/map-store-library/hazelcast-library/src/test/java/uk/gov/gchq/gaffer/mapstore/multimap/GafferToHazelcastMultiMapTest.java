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

import org.junit.Test;
import uk.gov.gchq.gaffer.store.StoreException;
import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GafferToHazelcastMultiMapTest {
    @Test
    public void shouldPutValueInExistingMapSet() throws StoreException {
        // Given
        final String key = "key1";
        final String value = "value1";
        final com.hazelcast.core.MultiMap<String, String> map = mock(com.hazelcast.core.MultiMap.class);
        final GafferToHazelcastMultiMap<String, String> multiMap = new GafferToHazelcastMultiMap<>(map);

        final boolean expectedResult = true;
        given(map.put(key, value)).willReturn(expectedResult);

        // When
        final boolean result = multiMap.put(key, value);

        // Then
        assertEquals(expectedResult, result);
        verify(map).put(key, value);
    }

    @Test
    public void shouldGetSetFromMap() throws StoreException {
        // Given
        final String key = "key1";
        final Set<String> set = mock(Set.class);
        final com.hazelcast.core.MultiMap<String, String> map = mock(com.hazelcast.core.MultiMap.class);
        final GafferToHazelcastMultiMap<String, String> multiMap = new GafferToHazelcastMultiMap<>(map);

        given(map.get(key)).willReturn(set);

        // When
        final Collection<String> result = multiMap.get(key);

        // Then
        verify(map).get(key);
        assertSame(set, result);
    }

    @Test
    public void shouldClearMap() throws StoreException {
        // Given
        final String key = "key1";
        final Set<String> set = mock(Set.class);
        final com.hazelcast.core.MultiMap<String, String> map = mock(com.hazelcast.core.MultiMap.class);
        final GafferToHazelcastMultiMap<String, String> multiMap = new GafferToHazelcastMultiMap<>(map);

        given(map.get(key)).willReturn(set);

        // When
        multiMap.clear();

        // Then
        verify(map).clear();
    }
}
