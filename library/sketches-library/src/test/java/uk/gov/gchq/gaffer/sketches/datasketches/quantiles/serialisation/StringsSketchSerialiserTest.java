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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation;

import com.yahoo.sketches.quantiles.ItemsSketch;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StringsSketchSerialiserTest {
    private static final double DELTA = 0.01D;
    private static final StringsSketchSerialiser SERIALISER = new StringsSketchSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final ItemsSketch<String> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
        sketch.update("A");
        sketch.update("B");
        sketch.update("C");
        testSerialiser(sketch);

        final ItemsSketch<String> emptySketch = ItemsSketch.getInstance(Comparator.naturalOrder());
        testSerialiser(emptySketch);
    }

    private void testSerialiser(final ItemsSketch<String> sketch) {
        final String quantile1 = sketch.getQuantile(0.5D);
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
        assertEquals(quantile1, sketchDeserialised.getQuantile(0.5D));
    }

    @Test
    public void testCanHandleDoublesUnion() {
        assertTrue(SERIALISER.canHandle(ItemsSketch.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }
}
