/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.BooleanSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serialiser to serialise and deserialise {@link Edge} objects in a byte array
 * representation.
 */
public class EdgeSerialiser extends PropertiesSerialiser<Edge> {
    private static final long serialVersionUID = 2205438497836765935L;
    private final BooleanSerialiser booleanSerialiser = new BooleanSerialiser();
    private final StringSerialiser stringSerialiser = new StringSerialiser();
    protected ToBytesSerialiser<Object> vertexSerialiser;

    // Required for serialisation
    EdgeSerialiser() {
        this.vertexSerialiser = null;
    }

    public EdgeSerialiser(final Schema schema) {
        super(schema);
    }

    @Override
    public void updateSchema(final Schema schema) {
        super.updateSchema(schema);
        if (null == schema.getVertexSerialiser()) {
            throw new IllegalArgumentException("Vertex serialiser is required");
        }
        if (!(schema.getVertexSerialiser() instanceof ToBytesSerialiser)) {
            throw new IllegalArgumentException("Vertex serialiser must be a " + ToBytesSerialiser.class.getSimpleName());
        }
        vertexSerialiser = (ToBytesSerialiser) schema.getVertexSerialiser();
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Edge.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final Edge edge) throws SerialisationException {
        final SchemaElementDefinition elementDefinition = schema.getElement(edge.getGroup());
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + edge.getGroup() + ", is this group in your schema?");
        }

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            LengthValueBytesSerialiserUtil.serialise(stringSerialiser, edge.getGroup(), out);
            LengthValueBytesSerialiserUtil.serialise(vertexSerialiser, edge.getSource(), out);
            LengthValueBytesSerialiserUtil.serialise(vertexSerialiser, edge.getDestination(), out);
            LengthValueBytesSerialiserUtil.serialise(booleanSerialiser, edge.isDirected(), out);
            serialiseProperties(edge.getProperties(), elementDefinition, out);
            return out.toByteArray();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to serialise edge into bytes", e);
        }
    }

    @Override
    public Edge deserialise(final byte[] bytes) throws SerialisationException {
        final int[] lastDelimiter = {0};
        final String group = LengthValueBytesSerialiserUtil.deserialise(stringSerialiser, bytes, lastDelimiter);
        final Object source = LengthValueBytesSerialiserUtil.deserialise(vertexSerialiser, bytes, lastDelimiter);
        final Object dest = LengthValueBytesSerialiserUtil.deserialise(vertexSerialiser, bytes, lastDelimiter);
        final boolean directed = LengthValueBytesSerialiserUtil.deserialise(booleanSerialiser, bytes, lastDelimiter);

        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema?");
        }

        final Edge edge = new Edge(group, source, dest, directed);
        deserialiseProperties(bytes, edge.getProperties(), elementDefinition, lastDelimiter);
        return edge;
    }

    @Override
    public Edge deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }
}
