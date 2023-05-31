/*
 * Copyright 2018-2023 Crown Copyright
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

import org.apache.datasketches.kll.KllFloatsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KllFloatsSketchSerialiserTest extends ViaCalculatedValueSerialiserTest<KllFloatsSketch, Float> {
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
        final KllFloatsSketch sketch = serialiser.deserialiseEmpty();

        // Then
        assertNull(sketch);
    }

    @Test
    public void testCanHandleKllFloatsSketch() {
        assertTrue(serialiser.canHandle(KllFloatsSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Override
    protected KllFloatsSketch getExampleOutput() {
        final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
        sketch.update(1.0F);
        sketch.update(2.0F);
        sketch.update(3.0F);
        return sketch;
    }

    @Override
    protected Float getTestValue(KllFloatsSketch object) {
        // Cannot get quantile for empty KllFloatsSketch
        if (object.isEmpty()) {
            return 0.0F;
        }
        return object.getQuantile(0.5D);
    }

    @Override
    protected KllFloatsSketch getEmptyExampleOutput() {
        return KllFloatsSketch.newHeapInstance();
    }

    @Override
    public Serialiser<KllFloatsSketch, byte[]> getSerialisation() {
        return new KllFloatsSketchSerialiser();
    }

    @Override
    public Pair<KllFloatsSketch, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{5, 1, 15, 0, -56, 0, 8, 0, 3, 0, 0, 0, 0, 0, 0, 0, -56, 0, 1, 0, -59, 0, 0, 0, 0, 0, -128, 63, 0, 0, 64, 64, 0, 0, 64, 64, 0, 0, 0, 64, 0, 0, -128, 63})};
    }
}
