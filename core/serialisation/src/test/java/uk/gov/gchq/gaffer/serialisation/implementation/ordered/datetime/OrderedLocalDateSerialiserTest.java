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

package uk.gov.gchq.gaffer.serialisation.implementation.ordered.datetime;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedLocalDateSerialiserTest extends ToBytesSerialisationTest<LocalDate> {

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

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 1000000L; i < 1001000L; i++) {
            final byte[] b = serialiser.serialise(LocalDate.ofEpochDay(i));
            final Object o = serialiser.deserialise(b);
            assertEquals(LocalDate.class, o.getClass());
            assertEquals(LocalDate.ofEpochDay(i), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        final byte[] b = serialiser.serialise(LocalDate.ofEpochDay(0));
        final Object o = serialiser.deserialise(b);
        assertEquals(LocalDate.class, o.getClass());
        assertEquals(LocalDate.ofEpochDay(0), o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() {
        assertTrue(serialiser.canHandle(LocalDate.class));
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        final LocalDate testDate = LocalDate.ofEpochDay(1L);
        final LocalDate aDayLater = testDate.plusDays(1L);
        assertTrue(compare(serialiser.serialise(testDate), serialiser.serialise(aDayLater)) < 0);
    }

    @Test
    public void checkMultipleDatesOrderPreserved() throws SerialisationException {
        LocalDate startTestDate = LocalDate.ofEpochDay(1);
        LocalDate newTestDate;
        for (Long time = 2L; time > 10L; time++) {
            newTestDate = LocalDate.ofEpochDay(time);
            assertTrue(compare(serialiser.serialise(startTestDate), serialiser.serialise(newTestDate)) < 0);
            startTestDate = newTestDate;
        }
    }

    @Override
    public Serialiser<LocalDate, byte[]> getSerialisation() {
        return new OrderedLocalDateSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<LocalDate, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(Instant.ofEpochSecond(60460074000000L)
                                  .atZone(ZoneId.systemDefault())
                                  .toLocalDate(), new byte[]{8, -128, 0, 0, 0, 41, -75, -94, 31}),
                new Pair<>(Instant.ofEpochSecond(61406234880000L)
                                  .atZone(ZoneId.systemDefault())
                                  .toLocalDate(), new byte[]{8, -128, 0, 0, 0, 42, 92, -69, 55}),
                new Pair<>(Instant.ofEpochSecond(59514676680000L)
                                  .atZone(ZoneId.systemDefault())
                                  .toLocalDate(), new byte[]{8, -128, 0, 0, 0, 41, 14, -85, -116})
        };
    }
}
