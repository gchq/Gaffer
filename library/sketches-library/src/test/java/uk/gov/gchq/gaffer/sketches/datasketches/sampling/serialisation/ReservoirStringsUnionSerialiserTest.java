/*
 * Copyright 2017 Crown Copyright
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

import com.yahoo.sketches.sampling.ReservoirItemsUnion;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedArrayValueSerialiserTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReservoirStringsUnionSerialiserTest extends ViaCalculatedArrayValueSerialiserTest<ReservoirItemsUnion<String>, String> {

    @Override
    protected ReservoirItemsUnion<String> getEmptyExampleOutput() {
        return ReservoirItemsUnion.newInstance(20);
    }

    @Override
    protected ReservoirItemsUnion<String> getExampleOutput() {
        final ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(20);
        union.update("1");
        union.update("2");
        union.update("3");
        return union;
    }

    @Override
    protected String[] getTestValue(final ReservoirItemsUnion<String> object) {
        return object.getResult().getSamples();
    }

    @Override
    public Serialiser<ReservoirItemsUnion<String>, byte[]> getSerialisation() {
        return new ReservoirStringsUnionSerialiser();
    }

    @SuppressWarnings("unchecked")
    public Pair<ReservoirItemsUnion<String>, byte[]>[] getHistoricSerialisationPairs() {
        ReservoirItemsUnion<String> union = getExampleOutput();
        return new Pair[]{new Pair(union, new byte[]{1, 2, 12, 0, 20, 0, 0, 0, -62, 2, 11, 0, 20, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 49, 1, 0, 0, 0, 50, 1, 0, 0, 0, 51})};
    }

    @Test
    public void testCanHandleReservoirItemsUnion() {
        assertTrue(serialiser.canHandle(ReservoirItemsUnion.class));
        assertFalse(serialiser.canHandle(String.class));
    }
}
