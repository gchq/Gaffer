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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.sampling;

import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;

import static org.junit.Assert.assertArrayEquals;

public class ReservoirNumbersSketchKryoSerializerTest extends KryoSerializerTest<ReservoirItemsSketch> {

    @Override
    protected void shouldCompareSerialisedAndDeserialisedObjects(final ReservoirItemsSketch obj, final ReservoirItemsSketch deserialised) {
        assertArrayEquals(obj.getSamples(), deserialised.getSamples());
    }

    @Override
    public Class<ReservoirItemsSketch> getTestClass() {
        return ReservoirItemsSketch.class;
    }

    @Override
    public ReservoirItemsSketch<Number> getTestObject() {
        final ReservoirItemsSketch<Number> sketch = ReservoirItemsSketch.newInstance(20);
        sketch.update(1L);
        sketch.update(2L);
        sketch.update(3L);
        return sketch;
    }
}
