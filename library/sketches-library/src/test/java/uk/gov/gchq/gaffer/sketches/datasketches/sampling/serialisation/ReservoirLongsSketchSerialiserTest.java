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

import org.apache.datasketches.sampling.ReservoirLongsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedArrayValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReservoirLongsSketchSerialiserTest extends ViaCalculatedArrayValueSerialiserTest<ReservoirLongsSketch, Long> {
    @Test
    public void testCanHandleReservoirLongsUnion() {
        assertTrue(serialiser.canHandle(ReservoirLongsSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }

    @Override
    protected ReservoirLongsSketch getExampleOutput() {
        final ReservoirLongsSketch union = ReservoirLongsSketch.newInstance(20);
        union.update(1L);
        union.update(2L);
        union.update(3L);
        return union;
    }

    @Override
    protected Long[] getTestValue(ReservoirLongsSketch object) {
        final long[] samples = object.getSamples();

        // Not ideal but this is test code, performance not important.
        final Long[] longs = new Long[samples.length];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = samples[i];
        }

        return longs;
    }

    @Override
    protected ReservoirLongsSketch getEmptyExampleOutput() {
        return ReservoirLongsSketch.newInstance(20);
    }

    @Override
    public Serialiser<ReservoirLongsSketch, byte[]> getSerialisation() {
        return new ReservoirLongsSketchSerialiser();
    }

    @Override
    public Pair<ReservoirLongsSketch, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleOutput(), new byte[]{-62, 2, 11, 0, 20, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0})};
    }
}
