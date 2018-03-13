/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DateSerialiserTest extends ToBytesSerialisationTest<Date> {


    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 121231232; i < (121231232 + 1000); i++) {
            byte[] b = serialiser.serialise(new Date(i));
            Object o = serialiser.deserialise(b);
            assertEquals(Date.class, o.getClass());
            assertEquals(new Date(i), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        byte[] b = serialiser.serialise(new Date(0));
        Object o = serialiser.deserialise(b);
        assertEquals(Date.class, o.getClass());
        assertEquals(new Date(0), o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(Date.class));
    }

    @Override
    public Serialiser getSerialisation() {
        return new DateSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Date, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(new Date(60460074000000L), new byte[]{54, 48, 52, 54, 48, 48, 55, 52, 48, 48, 48, 48, 48, 48}),
                new Pair<>(new Date(61406234880000L), new byte[]{54, 49, 52, 48, 54, 50, 51, 52, 56, 56, 48, 48, 48, 48}),
                new Pair<>(new Date(59514676680000L), new byte[]{53, 57, 53, 49, 52, 54, 55, 54, 54, 56, 48, 48, 48, 48})
        };
    }
}
