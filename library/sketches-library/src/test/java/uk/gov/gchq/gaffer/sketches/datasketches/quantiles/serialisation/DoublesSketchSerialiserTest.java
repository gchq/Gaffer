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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DoublesSketchSerialiserTest {
    private static final double DELTA = 0.01D;
    private static final DoublesSketchSerialiser SERIALISER = new DoublesSketchSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final UpdateDoublesSketch sketch = DoublesSketch.builder().build();
        sketch.update(1.0D);
        sketch.update(2.0D);
        sketch.update(3.0D);
        testSerialiser(sketch);

        final DoublesSketch emptySketch = DoublesSketch.builder().build();
        testSerialiser(emptySketch);
    }

    private void testSerialiser(final DoublesSketch sketch) {
        final double quantile1 = sketch.getQuantile(0.5D);
        final byte[] sketchSerialised;
        try {
            sketchSerialised = SERIALISER.serialise(sketch);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }

        final DoublesSketch sketchDeserialised;
        try {
            sketchDeserialised = SERIALISER.deserialise(sketchSerialised);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }
        assertEquals(quantile1, sketchDeserialised.getQuantile(0.5D), DELTA);
    }

    @Test
    public void testCanHandleDoublesUnion() {
        assertTrue(SERIALISER.canHandle(DoublesSketch.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }
}
