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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import uk.gov.gchq.gaffer.types.TypeValue;

/**
 * A Kryo {@link Serializer} for a {@link TypeValue}.
 */
public class TypeValueKryoSerializer extends Serializer<TypeValue> {

    @Override
    public void write(final Kryo kryo, final Output output, final TypeValue typeValue) {
        output.writeString(typeValue.getType());
        output.writeString(typeValue.getValue());
    }

    @Override
    public TypeValue read(final Kryo kryo, final Input input, final Class<TypeValue> aClass) {
        final String type = input.readString();
        final String value = input.readString();
        return new TypeValue(type, value);
    }
}
