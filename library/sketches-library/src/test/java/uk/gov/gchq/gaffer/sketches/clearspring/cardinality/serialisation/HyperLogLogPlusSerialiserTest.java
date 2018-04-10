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
package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HyperLogLogPlusSerialiserTest extends ViaCalculatedValueSerialiserTest<HyperLogLogPlus, Long> {

    @Override
    protected HyperLogLogPlus getEmptyExampleOutput() {
        return new HyperLogLogPlus.Builder(5, 5).build();
    }

    @Override
    protected HyperLogLogPlus getExampleOutput() {
        final HyperLogLogPlus hyperLogLogPlus1 = new HyperLogLogPlus(5, 5);
        hyperLogLogPlus1.offer("A");
        hyperLogLogPlus1.offer("B");
        return hyperLogLogPlus1;
    }

    @Override
    protected Long getTestValue(final HyperLogLogPlus object) {
        return object.cardinality();
    }

    @Test
    public void testSerialiseAndDeserialiseWhenEmpty() {
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(5, 5);
        long preSerialisationCardinality = hyperLogLogPlus.cardinality();
        byte[] hyperLogLogPlusSerialised;
        try {
            hyperLogLogPlusSerialised = serialiser.serialise(hyperLogLogPlus);
        } catch (final SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }
        HyperLogLogPlus hyperLogLogPlusDeserialised;
        try {
            hyperLogLogPlusDeserialised = serialiser.deserialise(hyperLogLogPlusSerialised);
        } catch (final SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }
        assertEquals(preSerialisationCardinality, hyperLogLogPlusDeserialised.cardinality());
    }

    @Test
    public void testSerialiseNullReturnsEmptyBytes() {
        // Given
        final byte[] hyperLogLogPlusSerialised = serialiser.serialiseNull();

        // Then
        assertArrayEquals(new byte[0], hyperLogLogPlusSerialised);
    }

    @Test
    public void testDeserialiseEmptyBytesReturnsNull() throws SerialisationException {
        // Given
        final HyperLogLogPlus hllp = serialiser.deserialiseEmpty();

        // Then
        assertNull(hllp);
    }

    @Test
    public void testCanHandleHyperLogLogPlus() {
        assertTrue(serialiser.canHandle(HyperLogLogPlus.class));
    }

    @Override
    public Serialiser<HyperLogLogPlus, byte[]> getSerialisation() {
        return new HyperLogLogPlusSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<HyperLogLogPlus, byte[]>[] getHistoricSerialisationPairs() {
        HyperLogLogPlus hyperLogLogPlus = getExampleOutput();
        return new Pair[]{new Pair(hyperLogLogPlus, new byte[]{-1, -1, -1, -2, 5, 5, 1, 2, -3, 6, -128, 11})};

    }
}
