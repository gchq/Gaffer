/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.types.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class FreqMapTests {

    private FreqMap freqMap;

    @Before
    public void initialiseFreqMap() {
        freqMap = new FreqMap();
    }

    @Test
    public void testUpsertCreatesNewKeyValue() {

        //given
        final String key = "test";
        final Integer value = 6;

        //when
        freqMap.upsert(key, value);

        //then
        assertTrue(freqMap.containsKey(key));
        assertEquals(value, freqMap.get(key));

    }

    @Test
    public void testUpsertUpdatesExistingKeyValue() {

        //given
        final String key = "test";
        final Integer initialValue = 3;
        final Integer increment = 11;
        final Integer expected = 14;
        freqMap.put(key, initialValue);

        //when
        freqMap.upsert(key, increment);

        //then
        assertEquals(freqMap.get(key), expected);

    }

    @Test
    public void testUpsertOverloadedCreateDefaultValue() {

        //given
        final String key = "test";
        final Integer expected = 1;

        //when
        freqMap.upsert(key);

        //then
        assertTrue(freqMap.containsKey(key));
        assertEquals(freqMap.get(key), expected);

    }

    @Test
    public void testUpsertOverloadedIncrementsDefaultValue() {

        //given
        final String key = "test";
        final Integer initialValue = 57;
        final Integer expected = 58;
        freqMap.upsert(key, initialValue);

        //when
        freqMap.upsert(key);

        //then
        assertEquals(freqMap.get(key), expected);
    }

    @Test
    public void testKeyExistsButValueNullIsHandled() {

        //given
        final String key = "test";
        freqMap.put(key, null);
        final Integer initialValue = 7;
        final Integer expectedValue = 8;

        //when
        freqMap.upsert(key, 7);
        freqMap.upsert(key);

        //then
        assertEquals(freqMap.get(key), expectedValue);
    }

}
