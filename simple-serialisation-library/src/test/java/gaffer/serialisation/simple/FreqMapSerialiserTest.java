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
package gaffer.serialisation.simple;

import gaffer.exception.SerialisationException;
import gaffer.serialisation.Serialisation;
import gaffer.serialisation.simple.constants.SimpleSerialisationConstants;
import gaffer.types.simple.FreqMap;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class FreqMapSerialiserTest {

    private static final FreqMapSerialiser SERIALISER = new FreqMapSerialiser();

    @Test
    public void canSerialiseEmptyFreqMap() throws SerialisationException {
        byte[] b = SERIALISER.serialise(new FreqMap());
        Object o = SERIALISER.deserialise(b);
        assertEquals(FreqMap.class, o.getClass());
        assertEquals(0, ((FreqMap)o).size());
    }

    @Test
    public void canSerialiseDeSerialiseFreqMapWithValues() throws SerialisationException {
        FreqMap freqMap = new FreqMap();
        freqMap.put("x", 10);
        freqMap.put("y", 5);
        freqMap.put("z", 20);
        byte[] b = SERIALISER.serialise(freqMap);
        FreqMap o = (FreqMap)SERIALISER.deserialise(b);
        assertEquals(FreqMap.class, o.getClass());
        assertEquals((Integer)10, o.get("x"));
        assertEquals((Integer)5, o.get("y"));
        assertEquals((Integer)20, o.get("z"));
    }

    @Test
    public void testSerialiserWillSkipEntryWithNullValue() throws SerialisationException {
        FreqMap freqMap = new FreqMap();
        freqMap.put("x", null);
        freqMap.put("y", 5);
        freqMap.put("z", 20);
        byte[] b = SERIALISER.serialise(freqMap);
        FreqMap o = (FreqMap)SERIALISER.deserialise(b);
        assertEquals(FreqMap.class, o.getClass());
        assertNull(o.get("x"));
        assertEquals((Integer)5, o.get("y"));
        assertEquals((Integer)20, o.get("z"));
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseFreqMap() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(FreqMap.class));
    }

}
