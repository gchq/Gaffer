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
import com.yahoo.sketches.sampling.ReservoirLongsUnion;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirLongsUnionSerialiser;

/**
 * A Kryo {@link Serializer} for an {@link ReservoirLongsUnion}.
 */
public class ReservoirLongsUnionKryoSerializer extends Serializer<ReservoirLongsUnion> {
    private ReservoirLongsUnionSerialiser serialiser;

    public ReservoirLongsUnionKryoSerializer() {
        this.serialiser = new ReservoirLongsUnionSerialiser();
    }

    @Override
    public void write(final Kryo kryo, final Output output, final ReservoirLongsUnion reservoirLongsUnion) {
        final byte[] serialised;
        try {
            serialised = serialiser.serialise(reservoirLongsUnion);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception serialising ReservoirLongsUnion to a byte array");
        }
        output.writeInt(serialised.length);
        output.writeBytes(serialised);
    }

    @Override
    public ReservoirLongsUnion read(final Kryo kryo, final Input input, final Class<ReservoirLongsUnion> type) {
        final int serialisedLength = input.readInt();
        final byte[] serialised = input.readBytes(serialisedLength);
        try {
            return serialiser.deserialise(serialised);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception deserialising ReservoirLongsUnion from a byte array", e);
        }
    }
}
