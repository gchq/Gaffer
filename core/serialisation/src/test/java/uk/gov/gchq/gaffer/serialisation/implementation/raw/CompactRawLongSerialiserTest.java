/*
 * Copyright 2016-2019 Crown Copyright
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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactRawLongSerialiserTest extends ToBytesSerialisationTest<Long> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 0; i < 1000; i++) {
            test(i);
        }
    }

    @Test
    public void testCanSerialiseANegativeSampleRange() throws SerialisationException {
        for (long i = -1000; i < 0; i++) {
            test(i);
        }
    }

    @Test
    public void canSerialiseLongMinValue() throws SerialisationException {
        test(Long.MIN_VALUE);
    }

    @Test
    public void canSerialiseLongMaxValue() throws SerialisationException {
        test(Long.MAX_VALUE);
    }

    @Test
    public void canSerialiseAllOrdersOfMagnitude() throws SerialisationException {
        for (int i = 0; i < 64; i++) {
            long value = (long) Math.pow(2, i);
            test(value);
            test(-value);
        }
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseLongClass() {
        assertTrue(serialiser.canHandle(Long.class));
    }

    private void test(final long value) throws SerialisationException {
        final byte[] b = serialiser.serialise(value);
        final Object o = ((ToBytesSerialiser) serialiser).deserialise(b, 0, b.length);
        assertEquals(Long.class, o.getClass());
        assertEquals(value, o);
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        CompactRawSerialisationUtils.write(value, new DataOutputStream(stream));
        final long result = CompactRawSerialisationUtils.read(new DataInputStream(new ByteArrayInputStream(stream.toByteArray())));
        assertEquals(result, value);
    }

    @Override
    public Serialiser<Long, byte[]> getSerialisation() {
        return new CompactRawLongSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Long, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(Long.MAX_VALUE, new byte[]{-120, 127, -1, -1, -1, -1, -1, -1, -1}),
                new Pair<>(Long.MIN_VALUE, new byte[]{-128, 127, -1, -1, -1, -1, -1, -1, -1}),
                new Pair<>(0L, new byte[]{0}),
                new Pair<>(1L, new byte[]{1})
        };
    }
}
