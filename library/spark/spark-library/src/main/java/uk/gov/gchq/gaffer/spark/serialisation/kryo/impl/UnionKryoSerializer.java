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
import com.yahoo.sketches.theta.Union;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.sketches.datasketches.theta.serialisation.UnionSerialiser;

/**
 * A Kryo {@link Serializer} for an {@link Union}.
 */
public class UnionKryoSerializer extends Serializer<Union> {
    private UnionSerialiser serialiser;

    public UnionKryoSerializer() {
        this.serialiser = new UnionSerialiser();
    }

    @Override
    public void write(final Kryo kryo, final Output output, final Union union) {
        final byte[] serialised;
        try {
            serialised = serialiser.serialise(union);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception serialising Union to a byte array");
        }
        output.writeInt(serialised.length);
        output.writeBytes(serialised);
    }

    @Override
    public Union read(final Kryo kryo, final Input input, final Class<Union> type) {
        final int serialisedLength = input.readInt();
        final byte[] serialised = input.readBytes(serialisedLength);
        try {
            return serialiser.deserialise(serialised);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception deserialising Union from a byte array", e);
        }
    }
}
