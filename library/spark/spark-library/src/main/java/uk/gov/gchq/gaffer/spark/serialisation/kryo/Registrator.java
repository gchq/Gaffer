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
import org.apache.spark.serializer.KryoRegistrator;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.EdgeKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.EntityKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.FreqMapKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.HyperLogLogPlusKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.TypeSubTypeValueKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.TypeValueKryoSerializer;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.types.TypeValue;

/**
 * A custom {@link KryoRegistrator} that serializes Gaffer {@link Entity}s and {@link Edge}s. NB: It
 * is not necessary to implement one for Elements as that is an abstract class.
 */
public class Registrator implements KryoRegistrator {

    @Override
    public void registerClasses(final Kryo kryo) {
        kryo.register(Entity.class, new EntityKryoSerializer());
        kryo.register(Edge.class, new EdgeKryoSerializer());
        kryo.register(Properties.class);
        kryo.register(FreqMap.class, new FreqMapKryoSerializer());
        kryo.register(HyperLogLogPlus.class, new HyperLogLogPlusKryoSerializer());
        kryo.register(TypeValue.class, new TypeValueKryoSerializer());
        kryo.register(TypeSubTypeValue.class, new TypeSubTypeValueKryoSerializer());
    }
}
