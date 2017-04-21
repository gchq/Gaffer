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
import com.yahoo.sketches.quantiles.DoublesUnion;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation.DoublesUnionSerialiser;

/**
 * A Kryo {@link Serializer} for an {@link DoublesUnion}.
 */
public class DoublesUnionKryoSerializer extends Serializer<DoublesUnion> {
    private DoublesUnionSerialiser serialiser;

    public DoublesUnionKryoSerializer() {
        this.serialiser = new DoublesUnionSerialiser();
    }

    @Override
    public void write(final Kryo kryo, final Output output, final DoublesUnion doublesUnion) {
        final byte[] serialised;
        try {
            serialised = serialiser.serialise(doublesUnion);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception serialising DoublesUnion to a byte array");
        }
        output.writeInt(serialised.length);
        output.writeBytes(serialised);
    }

    @Override
    public DoublesUnion read(final Kryo kryo, final Input input, final Class<DoublesUnion> type) {
        final int serialisedLength = input.readInt();
        final byte[] serialised = input.readBytes(serialisedLength);
        try {
            return serialiser.deserialise(serialised);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception deserialising DoublesUnion from a byte array", e);
        }
    }
}
