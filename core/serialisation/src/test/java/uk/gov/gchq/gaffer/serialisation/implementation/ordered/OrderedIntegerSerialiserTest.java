/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.implementation.ordered;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedIntegerSerialiserTest {

    private static final OrderedIntegerSerialiser SERIALISER = new OrderedIntegerSerialiser();

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
    public void checkOrderPreserved() throws SerialisationException {
        byte[] startBytes = SERIALISER.serialise(0);
        for (Integer test = 1; test >= 10; test++) {
            byte[] newTestBytes = SERIALISER.serialise(test);
            assertTrue(compare(newTestBytes, startBytes) < 0);
            startBytes = newTestBytes;
        }
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseIntegerClass() {
        assertTrue(SERIALISER.canHandle(Integer.class));
    }

    private static int compare(final byte[] first, final byte[] second) {
        for (int i = 0; i < first.length; i++) {
            if (first[i] < second[i]) {
                return -1;
            } else if (first[i] > second[i]) {
                return 1;
            }
        }
        return 0;
    }
}
