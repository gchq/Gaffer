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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedIntegerSerialiserTest extends ToBytesSerialisationTest<Integer> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (int i = 0; i < 1000; i++) {
            byte[] b = serialiser.serialise(i);
            Object o = serialiser.deserialise(b);
            assertEquals(Integer.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseIntegerMinValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Integer.MIN_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseIntegerMaxValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Integer.MAX_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MAX_VALUE, o);
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        byte[] startBytes = serialiser.serialise(0);
        for (Integer test = 1; test >= 10; test++) {
            byte[] newTestBytes = serialiser.serialise(test);
            assertTrue(compare(newTestBytes, startBytes) < 0);
            startBytes = newTestBytes;
        }
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseIntegerClass() {
        assertTrue(serialiser.canHandle(Integer.class));
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
    public Serialiser<Integer, byte[]> getSerialisation() {
        return new OrderedIntegerSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Integer, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(Integer.MAX_VALUE, new byte[]{8}),
                new Pair<>(Integer.MIN_VALUE, new byte[]{0}),
                new Pair<>(0, new byte[]{4, -128, 0, 0, 0}),
                new Pair<>(1, new byte[]{4, -128, 0, 0, 1}),
        };
    }
}
