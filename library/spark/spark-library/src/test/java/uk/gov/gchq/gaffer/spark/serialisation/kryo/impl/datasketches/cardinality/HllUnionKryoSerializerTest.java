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

import com.yahoo.sketches.hll.Union;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;

import static org.junit.Assert.assertEquals;

public class HllUnionKryoSerializerTest extends KryoSerializerTest<Union> {
    private static final double DELTA = 0.0000001D;

    @Override
    protected void shouldCompareSerialisedAndDeserialisedObjects(final Union obj, final Union deserialised) {
        assertEquals(obj.getEstimate(), deserialised.getEstimate(), DELTA);
    }

    @Override
    public Class<Union> getTestClass() {
        return Union.class;
    }

    @Override
    public Union getTestObject() {
        final Union union = new Union(15);
        union.update("A");
        union.update("B");
        union.update("C");
        return union;
    }
}
