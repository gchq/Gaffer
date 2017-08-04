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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation;

import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReservoirNumbersSketchSerialiserTest {
    private static final ReservoirNumbersSketchSerialiser SERIALISER = new ReservoirNumbersSketchSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final ReservoirItemsSketch<Number> sketch = ReservoirItemsSketch.newInstance(20);
        sketch.update(1L);
        sketch.update(2L);
        sketch.update(3L);
        testSerialiser(sketch);

        final ReservoirItemsSketch<Number> emptyUnion = ReservoirItemsSketch.newInstance(20);
        testSerialiser(emptyUnion);
    }

    private void testSerialiser(final ReservoirItemsSketch<Number> sketch) {
        final boolean resultIsNull = sketch == null;
        Number[] sample = new Number[]{};
        if (!resultIsNull) {
            sample = sketch.getSamples();
        }
        final byte[] sketchSerialised;
        try {
            sketchSerialised = SERIALISER.serialise(sketch);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }

        final ReservoirItemsSketch<Number> sketchDeserialised;
        try {
            sketchDeserialised = SERIALISER.deserialise(sketchSerialised);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }
        if (sketchDeserialised == null) {
            assertTrue(resultIsNull);
        } else {
            assertArrayEquals(sample, sketchDeserialised.getSamples());
        }
    }

    @Test
    public void testCanHandleReservoirItemsUnion() {
        assertTrue(SERIALISER.canHandle(ReservoirItemsSketch.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }
}
