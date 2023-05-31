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

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DoublesSketchSerialiserTest extends ViaCalculatedValueSerialiserTest<DoublesSketch, Double> {
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
        final DoublesSketch sketch = serialiser.deserialiseEmpty();

        // Then
        assertNull(sketch);
    }

    @Test
    public void testCanHandleDoublesUnion() {
        assertTrue(serialiser.canHandle(DoublesSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Override
    protected UpdateDoublesSketch getExampleOutput() {
        final UpdateDoublesSketch sketch = DoublesSketch.builder().build();
        sketch.update(1.0D);
        sketch.update(2.0D);
        sketch.update(3.0D);
        return sketch;
    }

    @Override
    protected Double getTestValue(DoublesSketch object) {
        // Cannot get quantile for empty DoublesSketch
        if (object.isEmpty()) {
            return 0.0D;
        }
        return object.getQuantile(0.5);
    }

    @Override
    protected UpdateDoublesSketch getEmptyExampleOutput() {
        return DoublesSketch.builder().build();
    }

    @Override
    public Serialiser<DoublesSketch, byte[]> getSerialisation() {
        return new DoublesSketchSerialiser();
    }

    @Override
    public Pair<DoublesSketch, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{2, 3, 8, 0, -128, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 0, 0})};
    }
}
