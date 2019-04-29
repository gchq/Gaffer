/*
 * Copyright 2016-2019 Crown Copyright
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * Serialiser to serialise and deserialise {@link ElementId} objects in a byte array
 * representation.
 */
public class ElementIdSerialiser implements ToBytesSerialiser<ElementId> {
    private static final long serialVersionUID = 2116809339334801784L;
    private final EntityIdSerialiser entityIdSerialiser;
    private final EdgeIdSerialiser edgeIdSerialiser;

    // Required for serialisation
    ElementIdSerialiser() {
        entityIdSerialiser = null;
        edgeIdSerialiser = null;
    }

    public ElementIdSerialiser(final Schema schema) {
        if (null == schema.getVertexSerialiser()) {
            throw new IllegalArgumentException("Vertex serialiser is required");
        }
        if (!(schema.getVertexSerialiser() instanceof ToBytesSerialiser)) {
            throw new IllegalArgumentException("Vertex serialiser must be a " + ToBytesSerialiser.class.getSimpleName());
        }
        entityIdSerialiser = new EntityIdSerialiser((ToBytesSerialiser) schema.getVertexSerialiser());
        edgeIdSerialiser = new EdgeIdSerialiser((ToBytesSerialiser) schema.getVertexSerialiser());
    }

    public ElementIdSerialiser(final ToBytesSerialiser vertexSerialiser) {
        entityIdSerialiser = new EntityIdSerialiser(vertexSerialiser);
        edgeIdSerialiser = new EdgeIdSerialiser(vertexSerialiser);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return ElementId.class.isAssignableFrom(clazz);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an ElementId is not an EntityId it must be an EdgeId")
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
        // Check how many delimiters there are. 1 = entityId. 3 = edgeId.
        final int nextDelimiter = LengthValueBytesSerialiserUtil.getNextDelimiter(bytes, 0);
        if (nextDelimiter < bytes.length) {
            return edgeIdSerialiser.deserialise(bytes);
        }

        return entityIdSerialiser.deserialise(bytes);
    }

    @Override
    public ElementId deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return null != edgeIdSerialiser && null != entityIdSerialiser
                && edgeIdSerialiser.preservesObjectOrdering() && entityIdSerialiser.preservesObjectOrdering();
    }

    @Override
    public boolean isConsistent() {
        return null != edgeIdSerialiser && null != entityIdSerialiser
                && edgeIdSerialiser.isConsistent() && entityIdSerialiser.isConsistent();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final ElementIdSerialiser serialiser = (ElementIdSerialiser) obj;

        return new EqualsBuilder()
                .append(entityIdSerialiser, serialiser.entityIdSerialiser)
                .append(edgeIdSerialiser, serialiser.edgeIdSerialiser)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(entityIdSerialiser)
                .append(edgeIdSerialiser)
                .toHashCode();
    }
}
