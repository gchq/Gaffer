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

import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class ElementIdSerialiser implements ToBytesSerialiser<ElementId> {
    private static final long serialVersionUID = 2116809339334801784L;
    private final EntityIdSerialiser entityIdSerialiser;
    private final EdgeIdSerialiser edgeIdSerialiser;

    public ElementIdSerialiser(final Schema schema) {
        this((ToBytesSerialiser) schema.getVertexSerialiser());
    }

    public ElementIdSerialiser(final ToBytesSerialiser vertexSerialiser) {
        entityIdSerialiser = new EntityIdSerialiser(vertexSerialiser);
        edgeIdSerialiser = new EdgeIdSerialiser(vertexSerialiser);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return ElementId.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final ElementId elementId) throws SerialisationException {
        if (null == elementId) {
            return new byte[0];
        }

        if (elementId instanceof EntityId) {
            return serialise((EntityId) elementId);
        }

        return serialise((EdgeId) elementId);
    }

    public byte[] serialise(final EntityId entityId) throws SerialisationException {
        return entityIdSerialiser.serialise(entityId);
    }

    public byte[] serialise(final EdgeId edgeId) throws SerialisationException {
        return edgeIdSerialiser.serialise(edgeId);
    }

    public byte[] serialiseVertex(final Object vertex) throws SerialisationException {
        return entityIdSerialiser.serialiseVertex(vertex);
    }

    @Override
    public ElementId deserialise(final byte[] bytes) throws SerialisationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ElementId deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
