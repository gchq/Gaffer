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

package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedRawDateSerialiserTest {

    private static final OrderedRawDateSerialiser SERIALISER = new OrderedRawDateSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 1000000L; i < 1001000L; i++) {
            final byte[] b = SERIALISER.serialise(new Date(i));
            final Object o = SERIALISER.deserialise(b);
            assertEquals(Date.class, o.getClass());
            assertEquals(new Date(i), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        final byte[] b = SERIALISER.serialise(new Date(0));
        final Object o = SERIALISER.deserialise(b);
        assertEquals(Date.class, o.getClass());
        assertEquals(new Date(0), o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() {
        assertTrue(SERIALISER.canHandle(Date.class));
    }

    @Test
    public void checkOrderPreserved() {
        Date testDate = new Date(1L);
        Date aDayLater = new Date(86400000L);
        assertTrue(compare(SERIALISER.serialise(testDate), SERIALISER.serialise(aDayLater)) < 0);
    }

    @Test
    public void checkMultipleDatesOrderPreserved() {
        Date startTestDate = new Date(1L);
        Date newTestDate;
        for (Long time = 2L; time > 10L; time++) {
            newTestDate = new Date(time);
            assertTrue(compare(SERIALISER.serialise(startTestDate), SERIALISER.serialise(newTestDate)) < 0);
            startTestDate = newTestDate;
        }
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
