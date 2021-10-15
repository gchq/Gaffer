/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FreqMapTests {

    private FreqMap freqMap;

    @BeforeEach
    public void initialiseFreqMap() {
        freqMap = new FreqMap();
    }

    @Test
    public void testUpsertCreatesNewKeyValue() {
        // Given
        final String key = "test";
        final Long value = 6L;

        // When
        freqMap.upsert(key, value);

        // Then
        assertTrue(freqMap.containsKey(key));
        assertEquals(value, freqMap.get(key));
    }

    @Test
    public void testUpsertUpdatesExistingKeyValue() {
        // Given
        final String key = "test";
        final Long initialValue = 3L;
        final Long increment = 11L;
        final Long expected = 14L;
        freqMap.put(key, initialValue);

        // When
        freqMap.upsert(key, increment);

        // Then
        assertEquals(freqMap.get(key), expected);
    }

    @Test
    public void testUpsertOverloadedCreateDefaultValue() {
        // Given
        final String key = "test";
        final Long expected = 1L;

        // When
        freqMap.upsert(key);

        // Then
        assertTrue(freqMap.containsKey(key));
        assertEquals(freqMap.get(key), expected);
    }

    @Test
    public void testUpsertOverloadedIncrementsDefaultValue() {
        // Given
        final String key = "test";
        final Long initialValue = 57L;
        final Long expected = 58L;
        freqMap.upsert(key, initialValue);

        // When
        freqMap.upsert(key);

        // Then
        assertEquals(freqMap.get(key), expected);
    }

    @Test
    public void testKeyExistsButValueNullIsHandled() {
        // Given
        final String key = "test";
        freqMap.put(key, null);
        final Long expectedValue = 8L;

        // When
        freqMap.upsert(key, 7L);
        freqMap.upsert(key);

        // Then
        assertEquals(freqMap.get(key), expectedValue);
    }
}
