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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.serialisation;

import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnionSerialiserTest extends ViaCalculatedValueSerialiserTest<Union, Double> {
    private static final double DELTA = 0.01D;

    @Override
    protected Union getEmptyExampleOutput() {
        return SetOperation.builder().buildUnion();
    }


    @Override
    public Serialiser<Union, byte[]> getSerialisation() {
        return new UnionSerialiser();
    }

    @SuppressWarnings("unchecked")
    public Pair<Union, byte[]>[] getHistoricSerialisationPairs() {
        Union union = getExampleOutput();
        return new Pair[]{new Pair(union, new byte[]{2, 3, 3, 0, 0, 26, -52, -109, 3, 0, 0, 0, 0, 0, -128, 63, 71, -94, 74, 125, 101, -74, 50, 27, 71, -54, -17, -50, 50, -91, 41, 29, -123, -46, -50, -27, -54, -41, -93, 124})};
    }

    @Override
    protected Union getExampleOutput() {
        final Union union = SetOperation.builder().buildUnion();
        union.update(1.0D);
        union.update(2.0D);
        union.update(3.0D);
        return union;
    }

    @Override
    protected Double getTestValue(final Union object) {
        return object.getResult().getEstimate();
//        assertEquals(estimate, unionDeserialised.getResult().getEstimate(), DELTA);
    }


    @Test
    public void testCanHandleUnion() {
        assertTrue(serialiser.canHandle(Union.class));
        assertFalse(serialiser.canHandle(String.class));
    }

}
