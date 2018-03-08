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

import com.yahoo.sketches.quantiles.ItemsSketch;

import uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation.StringsSketchSerialiser;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.WrappedKryoSerializer;

/**
 * A {@code StringsSketchKryoSerializer} is a {@link com.esotericsoftware.kryo.Kryo} {@link com.esotericsoftware.kryo.Serializer} for
 * an {@link ItemsSketch} of type {@link String}
 */
public class StringsSketchKryoSerializer extends WrappedKryoSerializer<StringsSketchSerialiser, ItemsSketch<String>> {

    public StringsSketchKryoSerializer() {
        super(new StringsSketchSerialiser());
    }
}
