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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation;

import com.yahoo.sketches.frequencies.ItemsSketch;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.ViaCalculatedValueSerialiserTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringsSketchSerialiserTest extends ViaCalculatedValueSerialiserTest<ItemsSketch<String>, Long> {


    @Override
    protected ItemsSketch<String> getEmptyExampleOutput() {
        return new ItemsSketch<>(32);
    }

    @Override
    public Serialiser<ItemsSketch<String>, byte[]> getSerialisation() {
        return new StringsSketchSerialiser();
    }

    @SuppressWarnings("unchecked")
    public Pair<ItemsSketch<String>, byte[]>[] getHistoricSerialisationPairs() {
        final ItemsSketch<String> sketch = getExampleOutput();
        return new Pair[]{new Pair(sketch, new byte[]{4, 1, 10, 5, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 49, 1, 0, 0, 0, 50, 1, 0, 0, 0, 51})};
    }

    @Override
    protected ItemsSketch<String> getExampleOutput() {
        final ItemsSketch<String> sketch = new ItemsSketch<>(32);
        sketch.update("1");
        sketch.update("2");
        sketch.update("3");
        return sketch;
    }

    @Override
    protected Long getTestValue(final ItemsSketch<String> value) {
        return value.getEstimate("1");
    }

    @Test
    public void testCanHandleItemsSketch() {
        assertTrue(serialiser.canHandle(ItemsSketch.class));
        assertFalse(serialiser.canHandle(String.class));
    }
}
