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

package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * For new properties use {@link uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedIntegerSerialiser}.
 * RawIntegerSerialiser serialises Integers into a little-endian byte array.
 *
 * @deprecated this is unable to preserve object ordering.
 * @see uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedIntegerSerialiser
 */
@Deprecated
public class RawIntegerSerialiser implements ToBytesSerialiser<Integer> {
    private static final long serialVersionUID = -8344193425875811395L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Integer.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Integer value) throws SerialisationException {
        final byte[] out = new byte[4];
        out[0] = (byte) ((value & 255));
        out[1] = (byte) ((value >> 8) & 255);
        out[2] = (byte) ((value >> 16) & 255);
        out[3] = (byte) ((value >> 24) & 255);
        return out;
    }

    @Override
    public Integer deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        int carriage = offset;
        return (int) (allBytes[carriage++] & 255L
                | (allBytes[carriage++] & 255L) << 8
                | (allBytes[carriage++] & 255L) << 16
                | (allBytes[carriage] & 255L) << 24);
    }

    @Override
    public Integer deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public Integer deserialiseEmpty() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }
}
