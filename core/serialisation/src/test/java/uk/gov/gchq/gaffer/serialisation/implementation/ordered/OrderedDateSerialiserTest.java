/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedDateSerialiserTest extends ToBytesSerialisationTest<Date> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 1000000L; i < 1001000L; i++) {
            final byte[] b = serialiser.serialise(new Date(i));
            final Object o = serialiser.deserialise(b);
            assertEquals(Date.class, o.getClass());
            assertEquals(new Date(i), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        final byte[] b = serialiser.serialise(new Date(0));
        final Object o = serialiser.deserialise(b);
        assertEquals(Date.class, o.getClass());
        assertEquals(new Date(0), o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() {
        assertTrue(serialiser.canHandle(Date.class));
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        Date testDate = new Date(1L);
        Date aDayLater = new Date(86400000L);
        assertTrue(compare(serialiser.serialise(testDate), serialiser.serialise(aDayLater)) < 0);
    }

    @Test
    public void checkMultipleDatesOrderPreserved() throws SerialisationException {
        Date startTestDate = new Date(1L);
        Date newTestDate;
        for (Long time = 2L; time > 10L; time++) {
            newTestDate = new Date(time);
            assertTrue(compare(serialiser.serialise(startTestDate), serialiser.serialise(newTestDate)) < 0);
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

    @Override
    public Serialiser<Date, byte[]> getSerialisation() {
        return new OrderedDateSerialiser();
    }


    @Override
    @SuppressWarnings("unchecked")
    public Pair<Date, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(new Date(60460074000000L), new byte[]{8, -128, 0, 54, -4, -11, 59, -34, -128}),
                new Pair<>(new Date(61406234880000L), new byte[]{8, -128, 0, 55, -39, 64, -47, 40, 0}),
                new Pair<>(new Date(59514676680000L), new byte[]{8, -128, 0, 54, 32, -41, 41, -107, 64})
        };
    }
}
