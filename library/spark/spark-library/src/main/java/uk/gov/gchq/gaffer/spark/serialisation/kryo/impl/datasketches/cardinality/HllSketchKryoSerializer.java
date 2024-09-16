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

package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.cardinality;

import org.apache.datasketches.hll.HllSketch;

import uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation.HllSketchSerialiser;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.WrappedKryoSerializer;

/**
 * A {@code HllSketchKryoSerializer} is a {@link com.esotericsoftware.kryo.Kryo} {@link com.esotericsoftware.kryo.Serializer} for
 * a {@link HllSketch}
 */
public class HllSketchKryoSerializer extends WrappedKryoSerializer<HllSketchSerialiser, HllSketch> {

    public HllSketchKryoSerializer() {
        super(new HllSketchSerialiser());
    }
}
