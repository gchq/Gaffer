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

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedArrayValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReservoirStringsSketchSerialiserTest extends ViaCalculatedArrayValueSerialiserTest<ReservoirItemsSketch<String>, String> {
    @Test
    public void testCanHandleReservoirItemsSketch() {
        assertTrue(serialiser.canHandle(ReservoirItemsSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void shouldHaveValidEqualsMethodForGenericSerialiser() {
        final Serialiser<ReservoirItemsSketch<String>, byte[]> serialiser2 = new ReservoirItemsSketchSerialiser<String>(new ArrayOfStringsSerDe());
        assertNotSame(this.serialiser, serialiser2,
                "The getSerialisation() shouldn't return the same instance each time it's called, required for this test.");
        assertEquals(this.serialiser, serialiser2, "different instances that are the same should be equal");
    }

    @Override
    protected ReservoirItemsSketch<String> getExampleOutput() {
        final ReservoirItemsSketch<String> sketch = ReservoirItemsSketch.newInstance(20);
        sketch.update("1");
        sketch.update("2");
        sketch.update("3");
        return sketch;
    }

    @Override
    protected String[] getTestValue(ReservoirItemsSketch<String> object) {
        return object.getSamples();
    }

    @Override
    protected ReservoirItemsSketch<String> getEmptyExampleOutput() {
        return ReservoirItemsSketch.newInstance(20);
    }

    @Override
    public Serialiser<ReservoirItemsSketch<String>, byte[]> getSerialisation() {
        return new ReservoirStringsSketchSerialiser();
    }

    @Override
    public Pair<ReservoirItemsSketch<String>, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{-62, 2, 11, 0, 20, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 49, 1, 0, 0, 0, 50, 1, 0, 0, 0, 51})};
    }
}
