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

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serialiser to serialise and deserialise {@link Entity} objects in a byte array
 * representation.
 */
public class EntitySerialiser extends PropertiesSerialiser<Entity> {
    private static final long serialVersionUID = -2582396256747930962L;
    private final StringSerialiser stringSerialiser = new StringSerialiser();
    protected ToBytesSerialiser<Object> vertexSerialiser;

    // Required for serialisation
    EntitySerialiser() {
        this.vertexSerialiser = null;
    }

    public EntitySerialiser(final Schema schema) {
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
        return Entity.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final Entity entity) throws SerialisationException {
        final SchemaElementDefinition elementDefinition = schema.getElement(entity.getGroup());
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + entity.getGroup() + ", is this group in your schema?");
        }

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            LengthValueBytesSerialiserUtil.serialise(stringSerialiser, entity.getGroup(), out);
            LengthValueBytesSerialiserUtil.serialise(vertexSerialiser, entity.getVertex(), out);
            serialiseProperties(entity.getProperties(), elementDefinition, out);
            return out.toByteArray();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to serialise entity into bytes", e);
        }
    }

    @Override
    public Entity deserialise(final byte[] bytes) throws SerialisationException {
        final int[] lastDelimiter = {0};
        final String group = LengthValueBytesSerialiserUtil.deserialise(stringSerialiser, bytes, lastDelimiter);
        final Object vertex = LengthValueBytesSerialiserUtil.deserialise(vertexSerialiser, bytes, lastDelimiter);

        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema?");
        }

        final Entity entity = new Entity(group, vertex);
        deserialiseProperties(bytes, entity.getProperties(), elementDefinition, lastDelimiter);
        return entity;
    }

    @Override
    public Entity deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }
}
