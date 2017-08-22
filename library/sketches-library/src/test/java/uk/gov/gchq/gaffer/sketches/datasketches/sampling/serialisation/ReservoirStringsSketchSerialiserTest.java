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

public class ReservoirStringsSketchSerialiserTest {
    private static final ReservoirStringsSketchSerialiser SERIALISER = new ReservoirStringsSketchSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final ReservoirItemsSketch<String> sketch = ReservoirItemsSketch.newInstance(20);
        sketch.update("1");
        sketch.update("2");
        sketch.update("3");
        testSerialiser(sketch);

        final ReservoirItemsSketch<String> emptySketch = ReservoirItemsSketch.newInstance(20);
        testSerialiser(emptySketch);
    }

    private void testSerialiser(final ReservoirItemsSketch<String> sketch) {
        final boolean resultIsNull = sketch == null;
        String[] sample = new String[]{};
        if (!resultIsNull) {
            sample = sketch.getSamples();
        }
        final byte[] unionSerialised;
        try {
            unionSerialised = SERIALISER.serialise(sketch);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }

        final ReservoirItemsSketch<String> sketchDeserialised;
        try {
            sketchDeserialised = SERIALISER.deserialise(unionSerialised);
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
    public void testCanHandleReservoirItemsSketch() {
        assertTrue(SERIALISER.canHandle(ReservoirItemsSketch.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }
}
