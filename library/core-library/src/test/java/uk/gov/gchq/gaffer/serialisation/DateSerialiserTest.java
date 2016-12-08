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
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DateSerialiserTest {

    private static final DateSerialiser SERIALISER = new DateSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 121231232; i < (121231232 + 1000); i++) {
            byte[] b = SERIALISER.serialise(new Date(i));
            Object o = SERIALISER.deserialise(b);
            assertEquals(Date.class, o.getClass());
            assertEquals(new Date(i), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        byte[] b = SERIALISER.serialise(new Date(0));
        Object o = SERIALISER.deserialise(b);
        assertEquals(Date.class, o.getClass());
        assertEquals(new Date(0), o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(Date.class));
    }

}