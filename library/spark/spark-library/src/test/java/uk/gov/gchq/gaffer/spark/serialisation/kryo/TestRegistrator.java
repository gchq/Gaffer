/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.serialisation.kryo;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.esotericsoftware.kryo.Kryo;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.EdgeKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.EntityKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.HyperLogLogPlusKryoSerializer;
import uk.gov.gchq.gaffer.types.FreqMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRegistrator {
    private final Kryo kryo = new Kryo();

    @Before
    public void setup() {
        new Registrator().registerClasses(kryo);
    }

    @Test
    public void testRegistered() {
        // Entity
        assertEquals(EntityKryoSerializer.class, kryo.getSerializer(Entity.class).getClass());

        // Edge
        assertEquals(EdgeKryoSerializer.class, kryo.getSerializer(Edge.class).getClass());

        // Properties
        assertTrue(kryo.getRegistration(Properties.class).getId() > 0);

        // FreqMap
        assertTrue(kryo.getRegistration(FreqMap.class).getId() > 0);

        // HyperLogLogPlus
        assertEquals(HyperLogLogPlusKryoSerializer.class, kryo.getSerializer(HyperLogLogPlus.class).getClass());
    }
}
