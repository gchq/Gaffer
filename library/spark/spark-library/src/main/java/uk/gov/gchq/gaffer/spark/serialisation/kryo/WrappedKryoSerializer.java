/*
 * Copyright 2017-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.serialisation.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code WrappedKryoSerializer} is a utility class for any {@link Kryo} {@link Serializer}
 * that wraps around any implementation of a Gaffer {@link ToBytesSerialiser}.
 * Implementations should simply use their constructor to call super with the wrapped {@link ToBytesSerialiser}
 * as the parameter.
 *
 * @param <S> the serialiser being wrapped
 * @param <T> the type for which the serialiser is to serialise
 */
public abstract class WrappedKryoSerializer<S extends ToBytesSerialiser<T>, T> extends Serializer<T> {
    protected S serialiser;

    public WrappedKryoSerializer(final S serialiser) {
        this.serialiser = serialiser;
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T obj) {
        final byte[] serialised;
        try {
            serialised = serialiser.serialise(obj);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception serialising "
                    + obj.getClass().getSimpleName()
                    + " to a byte array", e);
        }
        output.writeInt(serialised.length);
        output.writeBytes(serialised);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
        final int serialisedLength = input.readInt();
        final byte[] serialised = input.readBytes(serialisedLength);
        try {
            return serialiser.deserialise(serialised);
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Exception deserialising "
                    + type.getSimpleName()
                    + " to a byte array", e);
        }
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final WrappedKryoSerializer serialiser = (WrappedKryoSerializer) obj;

        return new EqualsBuilder()
                .append(serialiser, serialiser.serialiser)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(serialiser)
                .toHashCode();
    }
}
