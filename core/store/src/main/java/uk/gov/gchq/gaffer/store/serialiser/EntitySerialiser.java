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

package uk.gov.gchq.gaffer.store.serialiser;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class EntitySerialiser extends PropertiesSerialiser implements ToBytesSerialiser<Entity> {
    private static final long serialVersionUID = -2582396256747930962L;
    protected ToBytesSerialiser<Object> vertexSerialiser;

    // Required for serialisation
    EntitySerialiser() {
    }

    public EntitySerialiser(final Schema schema) {
        super(schema);
        if (null == schema.getVertexSerialiser()) {
            throw new IllegalArgumentException("Vertex serialiser is required");
        }
        if (!(schema.getVertexSerialiser() instanceof ToBytesSerialiser)) {
            throw new IllegalArgumentException("Vertex serialiser must be a " + ToBytesSerialiser.class.getSimpleName());
        }
        this.vertexSerialiser = (ToBytesSerialiser<Object>) schema.getVertexSerialiser();
    }

    @Override
    public void updateSchema(final Schema schema) {
        super.updateSchema(schema);
        if (null == schema.getVertexSerialiser()) {
            throw new IllegalArgumentException("Vertex serialiser must be defined in the schema");
        }
        if (!(schema.getVertexSerialiser() instanceof ToBytesSerialiser)) {
            throw new IllegalArgumentException("Vertex serialiser " + schema.getVertexSerialiser().getClass().getName() + " must be an instance of " + ToBytesSerialiser.class.getName());
        }
        vertexSerialiser = (ToBytesSerialiser) schema.getVertexSerialiser();
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Entity.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final Entity entity) throws SerialisationException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = schema.getElement(entity.getGroup());
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + entity.getGroup() + ", is this group in your schema or do your table iterators need updating?");
        }

        try {
            LengthValueBytesSerialiserUtil.serialise(StringUtil.toBytes(entity.getGroup()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise entity vertex to ByteArrayOutputStream", e);
        }

        try {
            LengthValueBytesSerialiserUtil.serialise(vertexSerialiser.serialise(entity.getVertex()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise entity vertex to ByteArrayOutputStream", e);
        }

        serialiseProperties(entity.getProperties(), elementDefinition, out);

        return out.toByteArray();
    }

    @Override
    public Entity deserialise(final byte[] bytes) throws SerialisationException {
        int lastDelimiter = 0;

        final byte[] groupBytes = LengthValueBytesSerialiserUtil.deserialise(bytes, lastDelimiter);
        final String group = StringUtil.toString(groupBytes);
        lastDelimiter = LengthValueBytesSerialiserUtil.getLastDelimiter(bytes, groupBytes, lastDelimiter);

        final byte[] vertexBytes = LengthValueBytesSerialiserUtil.deserialise(bytes, lastDelimiter);
        final Object vertex = ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(vertexBytes);
        lastDelimiter = LengthValueBytesSerialiserUtil.getLastDelimiter(bytes, vertexBytes, lastDelimiter);

        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final Entity entity = new Entity(group, vertex);
        deserialiseProperties(bytes, entity.getProperties(), elementDefinition, lastDelimiter);
        return entity;
    }

    @Override
    public Entity deserialiseEmpty() throws SerialisationException {
        return null;
    }
}
