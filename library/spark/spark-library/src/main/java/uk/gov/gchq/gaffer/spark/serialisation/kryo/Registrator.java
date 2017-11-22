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
import com.esotericsoftware.kryo.Registration;
import com.yahoo.sketches.frequencies.ItemsSketch;
import com.yahoo.sketches.frequencies.LongsSketch;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;
import com.yahoo.sketches.quantiles.CompactDoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import com.yahoo.sketches.sampling.ReservoirLongsSketch;
import com.yahoo.sketches.sampling.ReservoirLongsUnion;
import com.yahoo.sketches.theta.Sketch;
import org.apache.spark.serializer.KryoRegistrator;

import org.objenesis.instantiator.basic.NewInstanceInstantiator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.EdgeKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.EntityKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.FreqMapKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.HyperLogLogPlusKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.cardinality.HllSketchKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.cardinality.HllUnionKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.frequencies.LongsSketchKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.frequencies.StringsSketchKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.quantiles.DoublesSketchKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.quantiles.DoublesUnionKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.quantiles.StringsUnionKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.sampling.ReservoirLongsSketchKryoSerialiser;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.sampling.ReservoirLongsUnionKryoSerialiser;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.theta.SketchKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.theta.UnionKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.TypeSubTypeValueKryoSerializer;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.TypeValueKryoSerializer;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.types.TypeValue;
import java.util.ArrayList;
import java.util.List;

/**
 * A custom {@link KryoRegistrator} that serializes Gaffer {@link Entity}s and {@link Edge}s. NB: It
 * is not necessary to implement one for Elements as that is an abstract class.
 */
public class Registrator implements KryoRegistrator {

    private List<Registration> setup() {
        List<Registration> registrations = new ArrayList<>();
        Registration compactDoublesReg = new Registration(CompactDoublesSketch.class, new DoublesSketchKryoSerializer(), 200);
        compactDoublesReg.setInstantiator(new NewInstanceInstantiator<>(CompactDoublesSketch.class));
        registrations.add(compactDoublesReg);

        Registration updateDoublesReg = new Registration(UpdateDoublesSketch.class, new DoublesSketchKryoSerializer(), 201);
        updateDoublesReg.setInstantiator(new NewInstanceInstantiator<>(UpdateDoublesSketch.class));
        registrations.add(updateDoublesReg);

        return registrations;
    }

    @Override
    public void registerClasses(final Kryo kryo) {
        kryo.register(Entity.class, new EntityKryoSerializer());
        kryo.register(Edge.class, new EdgeKryoSerializer());
        kryo.register(Properties.class);
        kryo.register(FreqMap.class, new FreqMapKryoSerializer());
        kryo.register(HyperLogLogPlus.class, new HyperLogLogPlusKryoSerializer());
        kryo.register(HllSketch.class, new HllSketchKryoSerializer());
        kryo.register(Union.class, new HllUnionKryoSerializer());
        kryo.register(LongsSketch.class, new LongsSketchKryoSerializer());
        kryo.register(ItemsSketch.class, new StringsSketchKryoSerializer());
        kryo.register(CompactDoublesSketch.class, new DoublesSketchKryoSerializer());
        kryo.register(UpdateDoublesSketch.class, new DoublesSketchKryoSerializer());
        kryo.register(DoublesUnion.class, new DoublesUnionKryoSerializer());
        kryo.register(com.yahoo.sketches.quantiles.ItemsSketch.class, new uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.quantiles.StringsSketchKryoSerializer());
        kryo.register(ItemsUnion.class, new StringsUnionKryoSerializer());
        kryo.register(ReservoirLongsSketch.class, new ReservoirLongsSketchKryoSerialiser());
        kryo.register(ReservoirLongsUnion.class, new ReservoirLongsUnionKryoSerialiser());
        kryo.register(Sketch.class, new SketchKryoSerializer());
        kryo.register(com.yahoo.sketches.theta.Union.class, new UnionKryoSerializer());
        kryo.register(TypeValue.class, new TypeValueKryoSerializer());
        kryo.register(TypeSubTypeValue.class, new TypeSubTypeValueKryoSerializer());

        setup().forEach(kryo::register);
    }
}
