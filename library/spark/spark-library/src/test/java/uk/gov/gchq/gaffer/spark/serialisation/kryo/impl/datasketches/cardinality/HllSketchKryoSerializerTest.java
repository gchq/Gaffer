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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.cardinality;

import com.yahoo.sketches.hll.HllSketch;

import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;

import static org.junit.Assert.assertEquals;

public class HllSketchKryoSerializerTest extends KryoSerializerTest<HllSketch> {
    private static final double DELTA = 0.0000001D;

    @Override
    public Class<HllSketch> getTestClass() {
        return HllSketch.class;
    }

    @Override
    public HllSketch getTestObject() {
        final HllSketch sketch = new HllSketch(15);
        sketch.update("A");
        sketch.update("B");
        sketch.update("C");
        return sketch;
    }

    @Override
    protected void shouldCompareSerialisedAndDeserialisedObjects(final HllSketch obj, final HllSketch deserialised) {
        assertEquals(obj.getEstimate(), deserialised.getEstimate(), DELTA);
    }
}
