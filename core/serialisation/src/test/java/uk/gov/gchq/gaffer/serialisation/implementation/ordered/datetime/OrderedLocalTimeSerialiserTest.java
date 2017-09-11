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
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedLocalTimeSerialiserTest extends ToBytesSerialisationTest<LocalTime> {

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
        for (long i = 0; i < 86399; i++) {
            final byte[] b = serialiser.serialise(LocalTime.ofSecondOfDay(i));
            final Object o = serialiser.deserialise(b);
            assertEquals(LocalTime.class, o.getClass());
            assertEquals(LocalTime.ofSecondOfDay(i), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        final byte[] b = serialiser.serialise(LocalTime.ofSecondOfDay(0));
        final Object o = serialiser.deserialise(b);
        assertEquals(LocalTime.class, o.getClass());
        assertEquals(LocalTime.ofSecondOfDay(0), o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() {
        assertTrue(serialiser.canHandle(LocalTime.class));
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        final LocalTime testDate = LocalTime.ofSecondOfDay(1L);
        final LocalTime aSecondLater = testDate.plusSeconds(1L);

        assertTrue(compare(serialiser.serialise(testDate), serialiser.serialise(aSecondLater)) < 0);
    }

    @Test
    public void checkMultipleDatesOrderPreserved() throws SerialisationException {
        LocalTime startTestDate = LocalTime.ofSecondOfDay(1);
        LocalTime newTestDate;
        for (Long time = 2L; time > 10L; time++) {
            newTestDate = LocalTime.ofSecondOfDay(time);
            assertTrue(compare(serialiser.serialise(startTestDate), serialiser.serialise(newTestDate)) < 0);
            startTestDate = newTestDate;
        }
    }

    @Override
    public Serialiser<LocalTime, byte[]> getSerialisation() {
        return new OrderedLocalTimeSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<LocalTime, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(Instant.ofEpochSecond(60460074000000L)
                                  .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC))
                                  .toLocalTime(), new byte[]{0}),
                new Pair<>(Instant.ofEpochSecond(61406234880000L)
                                  .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC))
                                  .toLocalTime(), new byte[]{-114, 37, -128}),
                new Pair<>(Instant.ofEpochSecond(59514676680000L)
                                  .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC))
                                  .toLocalTime(), new byte[]{-114, -125, 64})
        };
    }
}
