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
package gaffer.serialisation.simple;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import gaffer.exception.SerialisationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HyperLogLogPlusSerialiserTest {

    final HyperLogLogPlusSerialiser hyperLogLogPlusSerialiser = new HyperLogLogPlusSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final HyperLogLogPlus hyperLogLogPlus1 = new HyperLogLogPlus(5, 5);
        hyperLogLogPlus1.offer("A");
        hyperLogLogPlus1.offer("B");


        long hyperLogLogPlus1PreSerialisationCardinality = hyperLogLogPlus1.cardinality();
        final byte[] hyperLogLogPlus1Serialised;
        try {
            hyperLogLogPlus1Serialised = hyperLogLogPlusSerialiser.serialise(hyperLogLogPlus1);
        } catch (SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }

        final HyperLogLogPlus hyperLogLogPlus1Deserialised;
        try {
            hyperLogLogPlus1Deserialised = hyperLogLogPlusSerialiser.deserialise(hyperLogLogPlus1Serialised);
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
            hyperLogLogPlusSerialised = hyperLogLogPlusSerialiser.serialise(hyperLogLogPlus);
        } catch (SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }
        HyperLogLogPlus hyperLogLogPlusDeserialised;
        try {
            hyperLogLogPlusDeserialised = hyperLogLogPlusSerialiser.deserialise(hyperLogLogPlusSerialised);
        } catch (SerialisationException exception) {
            fail("A Serialisation Exception Occurred");
            return;
        }
        assertEquals(preSerialisationCardinality, hyperLogLogPlusDeserialised.cardinality());
    }


    @Test
    public void testCanHandleHyperLogLogPlus() {
        assertTrue(hyperLogLogPlusSerialiser.canHandle(HyperLogLogPlus.class));
    }
}
