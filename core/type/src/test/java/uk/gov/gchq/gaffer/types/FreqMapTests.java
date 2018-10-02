/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.types;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.function.BiPredicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        final Long value = 6L;

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
        final Long initialValue = 3L;
        final Long increment = 11L;
        final Long expected = 14L;
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
        final Long expected = 1L;

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
        final Long initialValue = 57L;
        final Long expected = 58L;
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
        final Long initialValue = 7L;
        final Long expectedValue = 8L;

        //when
        freqMap.upsert(key, 7L);
        freqMap.upsert(key);

        //then
        assertEquals(freqMap.get(key), expectedValue);
    }

    @Test
    public void testRegexAndPredicates() {
        freqMap.upsert("cat");
        freqMap.upsert("cat");
        freqMap.upsert("dog");
        freqMap.upsert("cow");
        freqMap.upsert("cow");
        freqMap.upsert("catdog");
        freqMap.upsert("catdog");
        freqMap.upsert("catdog");
        freqMap.upsert("cat");
        freqMap.upsert("cat");

        //Check regex works
        FreqMap fRegex = freqMap.filterRegex("^\\wo\\w$");

        assertEquals(fRegex.size(), 2);
        assertFalse(fRegex.containsKey("cat"));
        assertTrue(fRegex.containsKey("cow"));

        //Again regex but in predicate form of key
        FreqMap fPredicateString = freqMap.filterPredicate((s, aLong) -> s.matches("^\\wo\\w$"));

        assertEquals(fRegex.size(), 2);
        assertFalse(fRegex.containsKey("cat"));
        assertTrue(fRegex.containsKey("cow"));

        //Value predicate
        FreqMap fPredicateLong = freqMap.filterPredicate((s, aLong) -> aLong > 1);

        assertEquals(fPredicateLong.size(), 3);
        assertFalse(fPredicateLong.containsKey("dog"));

        //Both value and key
        FreqMap fPredicateBoth = freqMap.filterPredicate((s, aLong) -> (s.matches("^\\wo\\w$") && aLong > 1));

        assertEquals(fPredicateBoth.size(), 1);
        assertFalse(fPredicateBoth.containsKey("dog"));
        assertTrue(fPredicateBoth.containsKey("cow"));

        //Tests to to see the freqMap was not manipulated itself
        assertEquals(freqMap.size(), 4);
        assertTrue(freqMap.containsKey("cat"));
        assertTrue(freqMap.containsKey("dog"));
        assertTrue(freqMap.containsKey("catdog"));
        assertTrue(freqMap.containsKey("cow"));
    }
}
