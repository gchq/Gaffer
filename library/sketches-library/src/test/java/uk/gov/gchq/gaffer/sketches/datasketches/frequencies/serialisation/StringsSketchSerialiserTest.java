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

import com.yahoo.sketches.frequencies.ItemsSketch;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StringsSketchSerialiserTest {
    private static final StringsSketchSerialiser SERIALISER = new StringsSketchSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final ItemsSketch<String> sketch = new ItemsSketch<>(32);
        sketch.update("1");
        sketch.update("2");
        sketch.update("3");
        testSerialiser(sketch);

        final ItemsSketch<String> emptySketch = new ItemsSketch<>(32);
        testSerialiser(emptySketch);
    }

    private void testSerialiser(final ItemsSketch<String> sketch) {
        final long freqOf1 = sketch.getEstimate("1");
        final byte[] sketchSerialised;
        try {
            sketchSerialised = SERIALISER.serialise(sketch);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }

        final ItemsSketch<String> sketchDeserialised;
        try {
            sketchDeserialised = SERIALISER.deserialise(sketchSerialised);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }
        assertEquals(freqOf1, sketchDeserialised.getEstimate("1"));
    }

    @Test
    public void testCanHandleItemsSketch() {
        assertTrue(SERIALISER.canHandle(ItemsSketch.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }
}
