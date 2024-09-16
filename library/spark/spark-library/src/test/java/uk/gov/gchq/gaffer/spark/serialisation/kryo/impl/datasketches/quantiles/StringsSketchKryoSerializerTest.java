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

package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.quantiles;

import org.apache.datasketches.quantiles.ItemsSketch;

import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringsSketchKryoSerializerTest extends KryoSerializerTest<ItemsSketch> {

    @Override
    protected void shouldCompareSerialisedAndDeserialisedObjects(final ItemsSketch obj, final ItemsSketch deserialised) {
        final double fraction = 0.5D;
        assertEquals(obj.getQuantile(fraction), deserialised.getQuantile(fraction));
    }

    @Override
    public Class<ItemsSketch> getTestClass() {
        return ItemsSketch.class;
    }

    @Override
    public ItemsSketch<String> getTestObject() {
        final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
        sketch.update("A");
        sketch.update("B");
        sketch.update("C");
        return sketch;
    }
}
