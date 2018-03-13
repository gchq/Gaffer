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

public class OrderedDoubleSerialiserTest extends ToBytesSerialisationTest<Double> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (double i = 0; i < 1000; i++) {
            byte[] b = serialiser.serialise(i);
            Object o = serialiser.deserialise(b);
            assertEquals(Double.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseDoubleMinValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Double.MIN_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseDoubleMaxValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Double.MAX_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MAX_VALUE, o);
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        byte[] startBytes = serialiser.serialise(0d);
        for (Double test = 1d; test >= 10d; test++) {
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
    public void canSerialiseDoubleClass() {
        assertTrue(serialiser.canHandle(Double.class));
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
    public Serialiser<Double, byte[]> getSerialisation() {
        return new OrderedDoubleSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Double, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(Double.MAX_VALUE, new byte[]{8, 127, -17, -1, -1, -1, -1, -1, -1}),
                new Pair<>(Double.MIN_VALUE, new byte[]{1, 1}),
                new Pair<>(0.0, new byte[]{0}),
                new Pair<>(1.00, new byte[]{8, 63, -16, 0, 0, 0, 0, 0, 0}),
        };
    }
}
