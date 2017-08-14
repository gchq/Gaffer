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
package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringSerialiserTest extends ToBytesSerialisationTest<String> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            builder.append(i);
            byte[] b = serialiser.serialise(builder.toString());
            Object o = serialiser.deserialise(b);
            assertEquals(String.class, o.getClass());
            assertEquals(builder.toString(), o);
        }
    }

    @Test
    public void cantSerialiseLongClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(Long.class));
    }

    @Test
    public void canSerialiseStringClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(String.class));
    }

    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        // When
        final String value = serialiser.deserialiseEmpty();

        // Then
        assertEquals("", value);
    }

    @Override
    public Serialiser<String, byte[]> getSerialisation() {
        return new StringSerialiser();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<String, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair("This is a test String ", new byte[]{84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 83, 116, 114, 105, 110, 103, 32}),
                new Pair("! @ # $ % ^ & * ( ) *  _ + { } [ ] ; : ' \\ | < , > . / ? ` ~", new byte[]{33, 32, 64, 32, 35, 32, 36, 32, 37, 32, 94, 32, 38, 32, 42, 32, 40, 32, 41, 32, 42, 32, 32, 95, 32, 43, 32, 123, 32, 125, 32, 91, 32, 93, 32, 59, 32, 58, 32, 39, 32, 92, 32, 124, 32, 60, 32, 44, 32, 62, 32, 46, 32, 47, 32, 63, 32, 96, 32, 126}),
                new Pair("œ ∑ ´ † ¥ ¨ ˆ π å ß ∂ ƒ © ˙ ∆ ˚ ¬ Ω≈ ç √ ∫ ˜  ≤¡ ™ £ ¢ ∞ § ¶ • ª º", new byte[]{-59, -109, 32, -30, -120, -111, 32, -62, -76, 32, -30, -128, -96, 32, -62, -91, 32, -62, -88, 32, -53, -122, 32, -49, -128, 32, -61, -91, 32, -61, -97, 32, -30, -120, -126, 32, -58, -110, 32, -62, -87, 32, -53, -103, 32, -30, -120, -122, 32, -53, -102, 32, -62, -84, 32, -50, -87, -30, -119, -120, 32, -61, -89, 32, -30, -120, -102, 32, -30, -120, -85, 32, -53, -100, 32, 32, -30, -119, -92, -62, -95, 32, -30, -124, -94, 32, -62, -93, 32, -62, -94, 32, -30, -120, -98, 32, -62, -89, 32, -62, -74, 32, -30, -128, -94, 32, -62, -86, 32, -62, -70}),
        };
    }

}