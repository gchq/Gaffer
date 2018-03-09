/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongsSketchSerialiserTest extends ViaCalculatedValueSerialiserTest<LongsSketch, Long> {

    @Override
    protected LongsSketch getEmptyExampleOutput() {
        return new LongsSketch(32);
    }

    @Override
    public Serialiser<LongsSketch, byte[]> getSerialisation() {
        return new LongsSketchSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<LongsSketch, byte[]>[] getHistoricSerialisationPairs() {
        final LongsSketch sketch = getExampleOutput();

        return new Pair[]{new Pair(sketch, new byte[]{4, 1, 10, 5, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})};
    }

    @Override
    protected LongsSketch getExampleOutput() {
        final LongsSketch sketch = new LongsSketch(32);
        sketch.update(1L);
        sketch.update(2L);
        sketch.update(3L);
        return sketch;
    }


    @Override
    protected Long getTestValue(final LongsSketch object) {
        return object.getEstimate(1L);
    }

    @Test
    public void testCanHandleLongsSketch() {
        assertTrue(serialiser.canHandle(LongsSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }
}
