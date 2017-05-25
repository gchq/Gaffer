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
package uk.gov.gchq.gaffer.serialisation;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.IntegerFreqMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IntegerFreqMapSerialiserTest {

    private static final IntegerFreqMapSerialiser SERIALISER = new IntegerFreqMapSerialiser();

    @Test
    public void canSerialiseEmptyFreqMap() throws SerialisationException {
        byte[] b = SERIALISER.serialise(new IntegerFreqMap());
        Object o = SERIALISER.deserialise(b);
        assertEquals(IntegerFreqMap.class, o.getClass());
        assertEquals(0, ((IntegerFreqMap) o).size());
    }

    @Test
    public void canSerialiseDeSerialiseFreqMapWithValues() throws SerialisationException {
        IntegerFreqMap freqMap = new IntegerFreqMap();
        freqMap.put("x", 10);
        freqMap.put("y", 5);
        freqMap.put("z", 20);
        byte[] b = SERIALISER.serialise(freqMap);
        IntegerFreqMap o = (IntegerFreqMap) SERIALISER.deserialise(b);
        assertEquals(IntegerFreqMap.class, o.getClass());
        assertEquals((Integer) 10, o.get("x"));
        assertEquals((Integer) 5, o.get("y"));
        assertEquals((Integer) 20, o.get("z"));
    }

    @Test
    public void testSerialiserWillSkipEntryWithNullValue() throws SerialisationException {
        IntegerFreqMap freqMap = new IntegerFreqMap();
        freqMap.put("x", null);
        freqMap.put("y", 5);
        freqMap.put("z", 20);
        byte[] b = SERIALISER.serialise(freqMap);
        IntegerFreqMap o = (IntegerFreqMap) SERIALISER.deserialise(b);
        assertEquals(IntegerFreqMap.class, o.getClass());
        assertNull(o.get("x"));
        assertEquals((Integer) 5, o.get("y"));
        assertEquals((Integer) 20, o.get("z"));
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseFreqMap() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(IntegerFreqMap.class));
    }

}
