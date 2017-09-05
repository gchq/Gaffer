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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Properties;

/**
 * A Kryo {@link Serializer} for an {@link Edge}.
 */
public class EdgeKryoSerializer extends Serializer<Edge> {

    @Override
    public void write(final Kryo kryo, final Output output, final Edge edge) {
        output.writeString(edge.getGroup());
        kryo.writeClass(output, edge.getSource().getClass());
        kryo.writeObject(output, edge.getSource());
        kryo.writeClass(output, edge.getDestination().getClass());
        kryo.writeObject(output, edge.getDestination());
        output.writeBoolean(edge.isDirected());
        kryo.writeObjectOrNull(output, edge.getProperties(), Properties.class);
    }

    @Override
    public Edge read(final Kryo kryo, final Input input, final Class<Edge> type) {
        final String group = input.readString();
        Registration reg = kryo.readClass(input);
        final Object source = kryo.readObject(input, reg.getType());
        reg = kryo.readClass(input);
        final Object dest = kryo.readObject(input, reg.getType());
        final boolean directed = input.readBoolean();
        final Properties properties = kryo.readObjectOrNull(input, Properties.class);
        return new Edge(group, source, dest, directed, null, properties);
    }
}
