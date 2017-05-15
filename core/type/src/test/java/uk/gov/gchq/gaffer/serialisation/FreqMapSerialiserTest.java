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
import uk.gov.gchq.gaffer.types.FreqMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FreqMapSerialiserTest extends SerialisationTest<FreqMap> {

    @Test
    public void canSerialiseEmptyFreqMap() throws SerialisationException {
        byte[] b = serialiser.serialise(new FreqMap());
        Object o = serialiser.deserialise(b);
        assertEquals(FreqMap.class, o.getClass());
        assertEquals(0, ((FreqMap) o).size());
    }

    @Test
    public void shouldSerialiseDeserialiseFreqMapWithValues() throws SerialisationException {
        // Given
        final FreqMap freqMap = new FreqMap();
        freqMap.put("x", 10L);
        freqMap.put("y", 5L);
        freqMap.put("z", 20L);

        // When
        final byte[] serialised = serialiser.serialise(freqMap);
        final FreqMap deserialised = (FreqMap) serialiser.deserialise(serialised);

        // Then
        assertEquals((Long) 10L, deserialised.get("x"));
        assertEquals((Long) 5L, deserialised.get("y"));
        assertEquals((Long) 20L, deserialised.get("z"));
    }

    @Test
    public void shouldSerialiseDeserialiseFreqMapWithAnEmptyKey() throws SerialisationException {
        // Given
        final FreqMap freqMap = new FreqMap();
        freqMap.put("", 10L);
        freqMap.put("y", 5L);
        freqMap.put("z", 20L);

        // When
        final byte[] serialised = serialiser.serialise(freqMap);
        final FreqMap deserialised = (FreqMap) serialiser.deserialise(serialised);

        assertEquals((Long) 10L, deserialised.get(""));
        assertEquals((Long) 5L, deserialised.get("y"));
        assertEquals((Long) 20L, deserialised.get("z"));
    }

    @Test
    public void shouldSkipEntryWithNullKey() throws SerialisationException {
        // Given
        final FreqMap freqMap = new FreqMap();
        freqMap.put(null, 10L);
        freqMap.put("y", 5L);
        freqMap.put("z", 20L);

        // When
        final byte[] serialised = serialiser.serialise(freqMap);
        final FreqMap deserialised = (FreqMap) serialiser.deserialise(serialised);

        assertFalse(deserialised.containsKey("x"));
        assertEquals((Long) 5L, deserialised.get("y"));
        assertEquals((Long) 20L, deserialised.get("z"));
    }

    @Test
    public void shouldSkipEntryWithNullValues() throws SerialisationException {
        // Given
        final FreqMap freqMap = new FreqMap();
        freqMap.put("v", null);
        freqMap.put("w", 5L);
        freqMap.put("x", null);
        freqMap.put("y", 20L);
        freqMap.put("z", null);

        // When
        final byte[] serialised = serialiser.serialise(freqMap);
        final FreqMap deserialised = (FreqMap) serialiser.deserialise(serialised);

        assertFalse(deserialised.containsKey("v"));
        assertEquals((Long) 5L, deserialised.get("w"));
        assertFalse(deserialised.containsKey("x"));
        assertEquals((Long) 20L, deserialised.get("y"));
        assertFalse(deserialised.containsKey("z"));
    }

    @Override
    public void shouldDeserialiseEmptyBytes() throws SerialisationException {
        // When
        final FreqMap value = serialiser.deserialiseEmptyBytes();

        // Then
        assertEquals(new FreqMap(), value);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseFreqMap() throws SerialisationException {
        assertTrue(serialiser.canHandle(FreqMap.class));
    }

    @Override
    public Serialisation<FreqMap> getSerialisation() {
        return new FreqMapSerialiser();
    }
}
