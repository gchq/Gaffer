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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.quantiles;

import com.google.common.collect.Ordering;
import com.yahoo.sketches.quantiles.ItemsUnion;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;

import static org.junit.Assert.assertEquals;

public class StringsUnionKryoSerializerTest extends KryoSerializerTest<ItemsUnion> {

    @Override
    protected void shouldCompareSerialisedAndDeserialisedObjects(final ItemsUnion obj, final ItemsUnion deserialised) {
        final double fraction = 0.5D;
        assertEquals(obj.getResult().getQuantile(fraction), deserialised.getResult().getQuantile(fraction));
    }

    @Override
    public Class<ItemsUnion> getTestClass() {
        return ItemsUnion.class;
    }

    @Override
    public ItemsUnion<String> getTestObject() {
        final ItemsUnion<String> union = ItemsUnion.getInstance(32, Ordering.<String>natural());
        union.update("1");
        union.update("2");
        union.update("3");
        return union;
    }
}
