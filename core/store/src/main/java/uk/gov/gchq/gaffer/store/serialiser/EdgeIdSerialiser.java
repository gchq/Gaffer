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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.BooleanSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class EdgeIdSerialiser implements ToBytesSerialiser<EdgeId> {
    private static final long serialVersionUID = -7123572023129773512L;
    protected final BooleanSerialiser booleanSerialiser = new BooleanSerialiser();
    protected final ToBytesSerialiser<Object> vertexSerialiser;

    // Required for serialisation
    EdgeIdSerialiser() {
        this.vertexSerialiser = null;
    }

    public EdgeIdSerialiser(final Schema schema) {
        if (null == schema.getVertexSerialiser()) {
            throw new IllegalArgumentException("Vertex serialiser is required");
        }
        if (!(schema.getVertexSerialiser() instanceof ToBytesSerialiser)) {
            throw new IllegalArgumentException("Vertex serialiser must be a " + ToBytesSerialiser.class.getSimpleName());
        }
        this.vertexSerialiser = (ToBytesSerialiser<Object>) schema.getVertexSerialiser();
    }

    public EdgeIdSerialiser(final ToBytesSerialiser vertexSerialiser) {
        this.vertexSerialiser = vertexSerialiser;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return EdgeId.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final EdgeId edgeId) throws SerialisationException {
        if (null == edgeId) {
            return new byte[0];
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            LengthValueBytesSerialiserUtil.serialise(vertexSerialiser.serialise(edgeId.getSource()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise edge vertex to ByteArrayOutputStream", e);
        }

        try {
            LengthValueBytesSerialiserUtil.serialise(vertexSerialiser.serialise(edgeId.getDestination()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise edge vertex to ByteArrayOutputStream", e);
        }

        try {
            LengthValueBytesSerialiserUtil.serialise(booleanSerialiser.serialise(edgeId.isDirected()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise edge vertex to ByteArrayOutputStream", e);
        }

        return out.toByteArray();
    }

    @Override
    public EdgeId deserialise(final byte[] bytes) throws SerialisationException {
        int lastDelimiter = 0;

        final byte[] sourceBytes = LengthValueBytesSerialiserUtil.deserialise(bytes, lastDelimiter);
        final Object source = (vertexSerialiser).deserialise(sourceBytes);
        lastDelimiter = LengthValueBytesSerialiserUtil.getLastDelimiter(bytes, sourceBytes, lastDelimiter);

        final byte[] destBytes = LengthValueBytesSerialiserUtil.deserialise(bytes, lastDelimiter);
        final Object dest = (vertexSerialiser).deserialise(destBytes);
        lastDelimiter = LengthValueBytesSerialiserUtil.getLastDelimiter(bytes, destBytes, lastDelimiter);

        final byte[] directedBytes = LengthValueBytesSerialiserUtil.deserialise(bytes, lastDelimiter);
        final boolean directed = booleanSerialiser.deserialise(directedBytes);

        return new EdgeSeed(source, dest, directed);
    }

    @Override
    public EdgeId deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
