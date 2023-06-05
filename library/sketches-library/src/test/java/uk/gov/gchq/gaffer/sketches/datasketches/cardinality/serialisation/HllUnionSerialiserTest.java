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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation;

import org.apache.datasketches.hll.Union;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HllUnionSerialiserTest extends ViaCalculatedValueSerialiserTest<Union, Double> {
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
        final Union sketch = serialiser.deserialiseEmpty();

        // Then
        assertNull(sketch);
    }

    @Test
    public void testCanHandleUnion() {
        assertTrue(serialiser.canHandle(Union.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Override
    protected Union getExampleOutput() {
        final Union sketch = new Union(15);
        sketch.update("A");
        sketch.update("B");
        sketch.update("C");
        return sketch;
    }

    @Override
    protected Double getTestValue(Union object) {
        return object.getEstimate();
    }

    @Override
    protected Union getEmptyExampleOutput() {
        return new Union(15);
    }

    @Override
    public Serialiser<Union, byte[]> getSerialisation() {
        return new HllUnionSerialiser();
    }

    @Override
    public Pair<Union, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{2, 1, 7, 15, 3, 8, 3, 0, 83, -25, -121, 5, 94, -114, -40, 5, 10, 68, 7, 11})};
    }
}
