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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation;

import com.yahoo.sketches.sampling.ReservoirLongsUnion;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedArrayValueSerialiserTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReservoirLongsUnionSerialiserTest extends ViaCalculatedArrayValueSerialiserTest<ReservoirLongsUnion, Long> {

    @Override
    protected ReservoirLongsUnion getEmptyExampleOutput() {
        return ReservoirLongsUnion.newInstance(20);
    }

    @Override
    public Serialiser<ReservoirLongsUnion, byte[]> getSerialisation() {
        return new ReservoirLongsUnionSerialiser();
    }

    @Override
    public Pair<ReservoirLongsUnion, byte[]>[] getHistoricSerialisationPairs() {
        final ReservoirLongsUnion union = getExampleOutput();
        return new Pair[]{new Pair(union, new byte[]{1, 2, 12, 0, 20, 0, 0, 0, -62, 2, 11, 0, 20, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0})};
    }

    @Override
    protected ReservoirLongsUnion getExampleOutput() {
        final ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(20);
        union.update(1L);
        union.update(2L);
        union.update(3L);
        return union;
    }

    @Override
    protected Long[] getTestValue(final ReservoirLongsUnion object) {
        final long[] samples = object.getResult().getSamples();

        // Not ideal but this is test code, performance not important.
        final Long[] longs = new Long[samples.length];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = samples[i];
        }

        return longs;
    }

    @Test
    public void testCanHandleReservoirLongsUnion() {
        assertTrue(serialiser.canHandle(ReservoirLongsUnion.class));
        assertFalse(serialiser.canHandle(String.class));
    }
}
