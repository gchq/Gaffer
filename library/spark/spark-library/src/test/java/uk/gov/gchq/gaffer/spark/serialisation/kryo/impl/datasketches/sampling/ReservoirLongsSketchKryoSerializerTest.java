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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.sampling;

import com.yahoo.sketches.sampling.ReservoirLongsSketch;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;

import static org.junit.Assert.assertArrayEquals;

public class ReservoirLongsSketchKryoSerializerTest extends KryoSerializerTest<ReservoirLongsSketch> {

    @Override
    protected void shouldCompareSerialisedAndDeserialisedObjects(final ReservoirLongsSketch obj, final ReservoirLongsSketch deserialised) {
        assertArrayEquals(obj.getSamples(), deserialised.getSamples());
    }

    @Override
    public Class<ReservoirLongsSketch> getTestClass() {
        return ReservoirLongsSketch.class;
    }

    @Override
    public ReservoirLongsSketch getTestObject() {
        final ReservoirLongsSketch union = ReservoirLongsSketch.newInstance(20);
        union.update(1L);
        union.update(2L);
        union.update(3L);
        return union;
    }
}
