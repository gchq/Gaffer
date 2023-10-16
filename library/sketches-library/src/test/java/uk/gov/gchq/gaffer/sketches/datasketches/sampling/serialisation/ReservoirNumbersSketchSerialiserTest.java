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

package uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation;

import org.apache.datasketches.common.ArrayOfNumbersSerDe;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedArrayValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReservoirNumbersSketchSerialiserTest extends ViaCalculatedArrayValueSerialiserTest<ReservoirItemsSketch<Number>, Number> {
    @Test
    public void testCanHandleReservoirItemsUnion() {
        assertTrue(serialiser.canHandle(ReservoirItemsSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void shouldHaveValidEqualsMethodForGenericSerialiser() {
        final Serialiser<ReservoirItemsSketch<Number>, byte[]> serialiser2 = new ReservoirItemsSketchSerialiser<Number>(new ArrayOfNumbersSerDe());
        assertNotSame(this.serialiser, serialiser2,
                "The getSerialisation() shouldn't return the same instance each time it's called, required for this test.");
        assertEquals(this.serialiser, serialiser2, "different instances that are the same should be equal");
    }

    @Override
    protected ReservoirItemsSketch<Number> getExampleOutput() {
        final ReservoirItemsSketch<Number> sketch = ReservoirItemsSketch.newInstance(20);
        sketch.update(1L);
        sketch.update(2L);
        sketch.update(3L);
        return sketch;
    }

    @Override
    protected Number[] getTestValue(ReservoirItemsSketch<Number> object) {
        return object.getSamples();
    }

    @Override
    protected ReservoirItemsSketch<Number> getEmptyExampleOutput() {
        return ReservoirItemsSketch.newInstance(20);
    }

    @Override
    public Serialiser<ReservoirItemsSketch<Number>, byte[]> getSerialisation() {
        return new ReservoirNumbersSketchSerialiser();
    }

    @Override
    public Pair<ReservoirItemsSketch<Number>, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{-62, 2, 11, 0, 20, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 12, 1, 0, 0, 0, 0, 0, 0, 0, 12, 2, 0, 0, 0, 0, 0, 0, 0, 12, 3, 0, 0, 0, 0, 0, 0, 0})};
    }
}
