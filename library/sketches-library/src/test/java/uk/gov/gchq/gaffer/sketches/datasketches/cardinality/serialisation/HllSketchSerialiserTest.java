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

import org.apache.datasketches.hll.HllSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HllSketchSerialiserTest extends ViaCalculatedValueSerialiserTest<HllSketch, Double> {
    @Test
    public void testSerialiseNullReturnsEmptyBytes() {
        // Given
        final byte[] hllSketchSerialised = serialiser.serialiseNull();

        // Then
        assertArrayEquals(new byte[0], hllSketchSerialised);
    }

    @Test
    public void testDeserialiseEmptyBytesReturnsNull() throws SerialisationException {
        // Given
        final HllSketch hllSketch = serialiser.deserialiseEmpty();

        // Then
        assertNull(hllSketch);
    }

    @Test
    public void testCanHandleHllSketch() {
        assertTrue(serialiser.canHandle(HllSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Override
    protected HllSketch getExampleOutput() {
        final HllSketch hllSketch = new HllSketch(15);
        hllSketch.update("A");
        hllSketch.update("B");
        hllSketch.update("C");
        return hllSketch;
    }

    @Override
    protected Double getTestValue(HllSketch object) {
        return object.getEstimate();
    }

    @Override
    protected HllSketch getEmptyExampleOutput() {
        return new HllSketch(15);
    }

    @Override
    public Serialiser<HllSketch, byte[]> getSerialisation() {
        return new HllSketchSerialiser();
    }

    @Override
    public Pair<HllSketch, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{2, 1, 7, 15, 3, 8, 3, 0, 83, -25, -121, 5, 94, -114, -40, 5, 10, 68, 7, 11})};
    }
}
