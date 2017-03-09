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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation;

import com.yahoo.sketches.frequencies.LongsSketch;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LongsSketchSerialiserTest {
    private static final LongsSketchSerialiser SERIALISER = new LongsSketchSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final LongsSketch sketch = new LongsSketch(32);
        sketch.update(1L);
        sketch.update(2L);
        sketch.update(3L);
        testSerialiser(sketch);

        final LongsSketch emptySketch = new LongsSketch(32);
        testSerialiser(emptySketch);
    }

    private void testSerialiser(final LongsSketch sketch) {
        final long freqOf1 = sketch.getEstimate(1L);
        final byte[] sketchSerialised;
        try {
            sketchSerialised = SERIALISER.serialise(sketch);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }

        final LongsSketch sketchDeserialised;
        try {
            sketchDeserialised = SERIALISER.deserialise(sketchSerialised);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }
        assertEquals(freqOf1, sketchDeserialised.getEstimate(1L));
    }

    @Test
    public void testCanHandleLongsSketch() {
        assertTrue(SERIALISER.canHandle(LongsSketch.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }
}
