/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.serialisation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HyperLogLogPlusSerialiserTest {
    private static final HyperLogLogPlusSerialiser HYPER_LOG_LOG_PLUS_SERIALISER = new HyperLogLogPlusSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final HyperLogLogPlus hyperLogLogPlus1 = new HyperLogLogPlus(5, 5);
        hyperLogLogPlus1.offer("A");
        hyperLogLogPlus1.offer("B");

        long hyperLogLogPlus1PreSerialisationCardinality = hyperLogLogPlus1.cardinality();
        final byte[] hyperLogLogPlus1Serialised;
        try {
            hyperLogLogPlus1Serialised = HYPER_LOG_LOG_PLUS_SERIALISER.serialise(hyperLogLogPlus1);
        } catch (SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }

        final HyperLogLogPlus hyperLogLogPlus1Deserialised;
        try {
            hyperLogLogPlus1Deserialised = HYPER_LOG_LOG_PLUS_SERIALISER.deserialise(hyperLogLogPlus1Serialised);
        } catch (SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }
        assertEquals(hyperLogLogPlus1PreSerialisationCardinality, hyperLogLogPlus1Deserialised.cardinality());
    }

    @Test
    public void testSerialiseAndDeserialiseWhenEmpty() {
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(5, 5);
        long preSerialisationCardinality = hyperLogLogPlus.cardinality();
        byte[] hyperLogLogPlusSerialised;
        try {
            hyperLogLogPlusSerialised = HYPER_LOG_LOG_PLUS_SERIALISER.serialise(hyperLogLogPlus);
        } catch (SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }
        HyperLogLogPlus hyperLogLogPlusDeserialised;
        try {
            hyperLogLogPlusDeserialised = HYPER_LOG_LOG_PLUS_SERIALISER.deserialise(hyperLogLogPlusSerialised);
        } catch (SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }
        assertEquals(preSerialisationCardinality, hyperLogLogPlusDeserialised.cardinality());
    }

    @Test
    public void testSerialiseNullReturnsEmptyBytes() {
        // Given
        final byte[] hyperLogLogPlusSerialised = HYPER_LOG_LOG_PLUS_SERIALISER.serialiseNull();

        // Then
        assertArrayEquals(new byte[0], hyperLogLogPlusSerialised);
    }

    @Test
    public void testDeserialiseEmptyBytesReturnsNull() throws SerialisationException {
        // Given
        final HyperLogLogPlus hllp = HYPER_LOG_LOG_PLUS_SERIALISER.deserialiseEmptyBytes();

        // Then
        assertNull(hllp);
    }

    @Test
    public void testCanHandleHyperLogLogPlus() {
        assertTrue(HYPER_LOG_LOG_PLUS_SERIALISER.canHandle(HyperLogLogPlus.class));
    }
}
