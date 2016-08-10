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

package gaffer.types.simple;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FreqMapTests {

    @Test
    public void testUpsertCreatesNewKeyValue(){

        //given
        FreqMap freqMap = new FreqMap();
        String key = "test";
        Integer value = 6;

        //when
        freqMap.upsert(key,value);

        //then
        assertTrue(freqMap.containsKey(key));
        assertEquals(value,freqMap.get(key));

    }

    @Test
    public void testUpsertUpdatesExistingKeyValue(){

        //given
        FreqMap freqMap = new FreqMap();
        String key = "test";
        Integer initialValue = 3;
        Integer increment = 11;
        Integer expected = 14;
        freqMap.put(key, initialValue);

        //when
        freqMap.upsert(key, increment);

        //then
        assertEquals(freqMap.get(key), expected);

    }

    @Test
    public void testUpsertOverloadedCreateDefaultValue(){

        //given
        FreqMap freqMap = new FreqMap();
        String key = "test";
        Integer expected = 1;

        //when
        freqMap.upsert(key);

        //then
        assertTrue(freqMap.containsKey(key));
        assertEquals(freqMap.get(key),expected);

    }

    @Test
    public void testUpsertOverloadedIncrementsDefaultValue(){

        //given
        FreqMap freqMap = new FreqMap();
        String key = "test";
        Integer initialValue = 57;
        Integer expected = 58;
        freqMap.upsert(key,initialValue);

        //when
        freqMap.upsert(key);

        //then
        assertEquals(freqMap.get(key),expected);
    }

}
