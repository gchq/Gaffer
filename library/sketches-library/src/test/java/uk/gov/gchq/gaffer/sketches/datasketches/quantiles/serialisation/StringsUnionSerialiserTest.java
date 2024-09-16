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

package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation;

import org.apache.datasketches.quantiles.ItemsUnion;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

public class StringsUnionSerialiserTest extends ViaCalculatedValueSerialiserTest<ItemsUnion<String>, String> {

    @Override
    protected ItemsUnion<String> getEmptyExampleOutput() {
        return ItemsUnion.getInstance(String.class, 32, Comparator.naturalOrder());
    }

    @Override
    public Serialiser<ItemsUnion<String>, byte[]> getSerialisation() {
        return new StringsUnionSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<ItemsUnion<String>, byte[]>[] getHistoricSerialisationPairs() {
        final ItemsUnion<String> union = getExampleOutput();
        testSerialiser(union);
        return new Pair[]{new Pair(union, new byte[]{2, 3, 8, 8, 32, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 49, 1, 0, 0, 0, 51, 1, 0, 0, 0, 49, 1, 0, 0, 0, 50, 1, 0, 0, 0, 51})};
    }

    @Override
    protected ItemsUnion<String> getExampleOutput() {
        final ItemsUnion<String> union = ItemsUnion.getInstance(String.class, 32, Comparator.naturalOrder());
        union.update("1");
        union.update("2");
        union.update("3");
        return union;
    }


    @Override
    protected String getTestValue(final ItemsUnion<String> object) {
        if (object.isEmpty()) {
            return null;
        }
        return object.getResult().getQuantile(0.5D);
    }

    @Test
    public void testCanHandleItemsUnion() {
        assertTrue(serialiser.canHandle(ItemsUnion.class));
        assertFalse(serialiser.canHandle(String.class));
    }
}
