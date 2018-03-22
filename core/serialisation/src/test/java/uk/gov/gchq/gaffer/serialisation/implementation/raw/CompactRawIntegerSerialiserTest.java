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
package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactRawIntegerSerialiserTest extends ToBytesSerialisationTest<Integer> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (int i = 0; i < 1000; i++) {
            test(i);
        }
    }

    @Test
    public void testCanSerialiseANegativeSampleRange() throws SerialisationException {
        for (int i = -1000; i < 0; i++) {
            test(i);
        }
    }

    @Test
    public void canSerialiseIntegerMinValue() throws SerialisationException {
        test(Integer.MIN_VALUE);
    }

    @Test
    public void canSerialiseIntegerMaxValue() throws SerialisationException {
        test(Integer.MAX_VALUE);
    }

    @Test
    public void canSerialiseAllOrdersOfMagnitude() throws SerialisationException {
        for (int i = 0; i < 32; i++) {
            int value = (int) Math.pow(2, i);
            test(value);
            test(-value);
        }
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseIntegerClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(Integer.class));
    }

    private void test(final int value) throws SerialisationException {
        final byte[] b = serialiser.serialise(value);
        final Object o = serialiser.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(value, o);
    }

    @Override
    public Serialiser<Integer, byte[]> getSerialisation() {
        return new CompactRawIntegerSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Integer, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(Integer.MAX_VALUE, new byte[]{-116, 127, -1, -1, -1}),
                new Pair<>(Integer.MIN_VALUE, new byte[]{-124, 127, -1, -1, -1}),
                new Pair<>(0, new byte[]{0}),
                new Pair<>(1, new byte[]{1})
        };
    }
}
