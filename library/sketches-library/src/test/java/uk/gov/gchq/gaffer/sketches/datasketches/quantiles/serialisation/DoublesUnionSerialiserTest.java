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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation;

import com.yahoo.sketches.quantiles.DoublesUnion;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DoublesUnionSerialiserTest extends ViaCalculatedValueSerialiserTest<DoublesUnion, Double> {
    private static final double DELTA = 0.01D;

    @Override
    public Serialiser<DoublesUnion, byte[]> getSerialisation() {
        return new DoublesUnionSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<DoublesUnion, byte[]>[] getHistoricSerialisationPairs() {
        final DoublesUnion union = getExampleOutput();
        return new Pair[]{new Pair(union, new byte[]{2, 3, 8, 0, -128, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 0, 0})};
    }


    @Override
    protected DoublesUnion getExampleOutput() {
        final DoublesUnion union = DoublesUnion.builder().build();
        union.update(1.0D);
        union.update(2.0D);
        union.update(3.0D);
        return union;
    }

    @Override
    protected Double getTestValue(final DoublesUnion object) {
        return object.getResult().getQuantile(0.5D);
//        assertEquals(quantile1, unionDeserialised.getResult().getQuantile(0.5D), DELTA);
    }

    @Override
    protected DoublesUnion getEmptyExampleOutput() {
        return DoublesUnion.builder().build();
    }

    @Test
    public void testCanHandleDoublesUnion() {
        assertTrue(serialiser.canHandle(DoublesUnion.class));
        assertFalse(serialiser.canHandle(String.class));
    }
}
