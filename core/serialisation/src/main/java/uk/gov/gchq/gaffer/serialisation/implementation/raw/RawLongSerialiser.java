/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

/**
 * RawLongSerialiser serialises Longs into a little-endian byte array.
 */
public class RawLongSerialiser implements Serialisation<Long> {
    private static final long serialVersionUID = 369129707952407270L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Long.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Long value) throws SerialisationException {
        final byte[] out = new byte[8];
        out[0] = (byte) ((int) (value & 255));
        out[1] = (byte) ((int) (value >> 8) & 255);
        out[2] = (byte) ((int) (value >> 16) & 255);
        out[3] = (byte) ((int) (value >> 24) & 255);
        out[4] = (byte) ((int) (value >> 32) & 255);
        out[5] = (byte) ((int) (value >> 40) & 255);
        out[6] = (byte) ((int) (value >> 48) & 255);
        out[7] = (byte) ((int) (value >> 56) & 255);
        return out;
    }

    @Override
    public Long deserialise(final byte[] bytes) throws SerialisationException {
        return (long) bytes[0] & 255L
                | ((long) bytes[1] & 255L) << 8
                | ((long) bytes[2] & 255L) << 16
                | ((long) bytes[3] & 255L) << 24
                | ((long) bytes[4] & 255L) << 32
                | ((long) bytes[5] & 255L) << 40
                | ((long) bytes[6] & 255L) << 48
                | ((long) bytes[7] & 255L) << 56;
    }

    @Override
    public Long deserialiseEmptyBytes() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }
}
