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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;

/**
 * A Kryo {@link Serializer} for an {@link Entity}.
 */
public class EntityKryoSerializer extends Serializer<Entity> {

    @Override
    public void write(final Kryo kryo, final Output output, final Entity entity) {
        output.writeString(entity.getGroup());
        kryo.writeClass(output, entity.getVertex().getClass());
        kryo.writeObject(output, entity.getVertex());
        kryo.writeObjectOrNull(output, entity.getProperties(), Properties.class);
    }

    @Override
    public Entity read(final Kryo kryo, final Input input, final Class<Entity> type) {
        final String group = input.readString();
        final Entity entity = new Entity(group);
        final Registration reg = kryo.readClass(input);
        entity.setVertex(kryo.readObject(input, reg.getType()));
        entity.copyProperties(kryo.readObjectOrNull(input, Properties.class));
        return entity;
    }
}
