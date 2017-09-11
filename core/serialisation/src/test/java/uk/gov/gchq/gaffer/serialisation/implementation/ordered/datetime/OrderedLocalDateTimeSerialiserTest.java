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

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedLocalDateTimeSerialiserTest extends ToBytesSerialisationTest<LocalDateTime> {

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
            final byte[] b = serialiser.serialise(LocalDateTime.ofEpochSecond(i, 0, ZoneOffset.UTC));
            final Object o = serialiser.deserialise(b);
            assertEquals(LocalDateTime.class, o.getClass());
            assertEquals(LocalDateTime.ofEpochSecond(i, 0, ZoneOffset.UTC), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        final byte[] b = serialiser.serialise(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC));
        final Object o = serialiser.deserialise(b);
        assertEquals(LocalDateTime.class, o.getClass());
        assertEquals(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC), o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() {
        assertTrue(serialiser.canHandle(LocalDateTime.class));
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        final LocalDateTime testDate = LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC);
        final LocalDateTime aDayLater = testDate.plusDays(1L);
        assertTrue(compare(serialiser.serialise(testDate), serialiser.serialise(aDayLater)) < 0);
    }

    @Test
    public void checkMultipleDatesOrderPreserved() throws SerialisationException {
        LocalDateTime startTestDate = LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC);
        LocalDateTime newTestDate;
        for (Long time = 2L; time > 10L; time++) {
            newTestDate = LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.UTC);
            assertTrue(compare(serialiser.serialise(startTestDate), serialiser.serialise(newTestDate)) < 0);
            startTestDate = newTestDate;
        }
    }

    @Override
    public Serialiser<LocalDateTime, byte[]> getSerialisation() {
        return new OrderedLocalDateTimeSerialiser();
    }


    @Override
    @SuppressWarnings("unchecked")
    public Pair<LocalDateTime, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(LocalDateTime.ofEpochSecond(60460074000000L, 0, ZoneOffset.UTC), new byte[]{8, -128, 0, 54, -4, -11, 59, -34, -128}),
                new Pair<>(LocalDateTime.ofEpochSecond(61406234880000L, 0, ZoneOffset.UTC), new byte[]{8, -128, 0, 55, -39, 64, -47, 40, 0}),
                new Pair<>(LocalDateTime.ofEpochSecond(59514676680000L, 0, ZoneOffset.UTC), new byte[]{8, -128, 0, 54, 32, -41, 41, -107, 64})
        };
    }
}
