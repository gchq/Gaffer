/*
 * Copyright 2017-2023 Crown Copyright
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

import org.apache.datasketches.quantiles.ItemsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringsSketchSerialiserTest extends ViaCalculatedValueSerialiserTest<ItemsSketch<String>, String> {
    @Test
    public void testSerialiseNullReturnsEmptyBytes() {
        // Given
        final byte[] sketchSerialised = serialiser.serialiseNull();

        // Then
        assertArrayEquals(new byte[0], sketchSerialised);
    }

    @Test
    public void testDeserialiseEmptyBytesReturnsNull() throws SerialisationException {
        // Given
        final ItemsSketch<String> sketch = serialiser.deserialiseEmpty();

        // Then
        assertNull(sketch);
    }

    @Test
    public void testCanHandleDoublesUnion() {
        assertTrue(serialiser.canHandle(ItemsSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Override
    protected ItemsSketch<String> getExampleOutput() {
        final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
        sketch.update("A");
        sketch.update("B");
        sketch.update("C");
        return sketch;
    }

    @Override
    protected String getTestValue(ItemsSketch<String> object) {
        // Cannot get quantile for empty ItemsSketch
        if (object.isEmpty()) {
            return "";
        }
        return object.getQuantile(0.5D);
    }

    @Override
    protected ItemsSketch<String> getEmptyExampleOutput() {
        return ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
    }

    @Override
    public Serialiser<ItemsSketch<String>, byte[]> getSerialisation() {
        return new StringsSketchSerialiser();
    }

    @Override
    public Pair<ItemsSketch<String>, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{2, 3, 8, 8, -128, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 65, 1, 0, 0, 0, 67, 1, 0, 0, 0, 65, 1, 0, 0, 0, 66, 1, 0, 0, 0, 67})};
    }
}
