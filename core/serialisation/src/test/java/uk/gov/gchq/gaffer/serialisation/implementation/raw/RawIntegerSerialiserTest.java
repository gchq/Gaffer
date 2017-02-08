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
package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RawIntegerSerialiserTest {

    private static final RawIntegerSerialiser SERIALISER = new RawIntegerSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (int i = 0; i < 1000; i++) {
            byte[] b = SERIALISER.serialise(i);
            Object o = SERIALISER.deserialise(b);
            assertEquals(Integer.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseIntegerMinValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Integer.MIN_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseIntegerMaxValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Integer.MAX_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseIntegerClass() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(Integer.class));
    }

}