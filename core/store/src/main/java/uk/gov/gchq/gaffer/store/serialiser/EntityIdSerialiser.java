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

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * Serialiser to serialise and deserialise {@link EntityId} objects in a byte array
 * representation.
 */
public class EntityIdSerialiser implements ToBytesSerialiser<EntityId> {
    private static final long serialVersionUID = -8190219367679033911L;
    protected final ToBytesSerialiser<Object> vertexSerialiser;

    // Required for serialisation
    EntityIdSerialiser() {
        this.vertexSerialiser = null;
    }

    public EntityIdSerialiser(final Schema schema) {
        if (null == schema.getVertexSerialiser()) {
            throw new IllegalArgumentException("Vertex serialiser is required");
        }
        if (!(schema.getVertexSerialiser() instanceof ToBytesSerialiser)) {
            throw new IllegalArgumentException("Vertex serialiser must be a " + ToBytesSerialiser.class.getSimpleName());
        }
        this.vertexSerialiser = (ToBytesSerialiser<Object>) schema.getVertexSerialiser();
    }

    public EntityIdSerialiser(final ToBytesSerialiser vertexSerialiser) {
        this.vertexSerialiser = vertexSerialiser;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return EntityId.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final EntityId entityId) throws SerialisationException {
        if (null == entityId) {
            return new byte[0];
        }
        return LengthValueBytesSerialiserUtil.serialise(vertexSerialiser, entityId.getVertex());
    }

    public byte[] serialiseVertex(final Object vertex) throws SerialisationException {
        return LengthValueBytesSerialiserUtil.serialise(vertexSerialiser.serialise(vertex));
    }

    @Override
    public EntityId deserialise(final byte[] bytes) throws SerialisationException {
        final Object vertex = LengthValueBytesSerialiserUtil.deserialise(vertexSerialiser, bytes, 0);
        return new EntitySeed(vertex);
    }

    @Override
    public EntityId deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return null != vertexSerialiser && vertexSerialiser.preservesObjectOrdering();
    }

    @Override
    public boolean isConsistent() {
        return null != vertexSerialiser && vertexSerialiser.isConsistent();
    }
}
