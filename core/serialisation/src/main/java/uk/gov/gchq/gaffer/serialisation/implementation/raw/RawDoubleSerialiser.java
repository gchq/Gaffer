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

package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * For new properties use {@link uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDoubleSerialiser}.
 * RawDoubleSerialiser serialises Doubles into an IEEE floating point little-endian byte array.
 *
 * @deprecated this is unable to preserve object ordering.
 * @see uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDoubleSerialiser
 */
@Deprecated
public class RawDoubleSerialiser implements ToBytesSerialiser<Double> {
    private static final long serialVersionUID = 1568251281744704278L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Double.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Double d) throws SerialisationException {
        final byte[] out = new byte[8];
        final long value = Double.doubleToRawLongBits(d);
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
    public Double deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public Double deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        int carriage = offset;
        return Double.longBitsToDouble(allBytes[carriage++] & 255L
                | ((long) allBytes[carriage++] & 255L) << 8
                | ((long) allBytes[carriage++] & 255L) << 16
                | ((long) allBytes[carriage++] & 255L) << 24
                | ((long) allBytes[carriage++] & 255L) << 32
                | ((long) allBytes[carriage++] & 255L) << 40
                | ((long) allBytes[carriage++] & 255L) << 48
                | ((long) allBytes[carriage] & 255L) << 56);

    }

    @Override
    public Double deserialiseEmpty() {
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

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return RawDoubleSerialiser.class.getName().hashCode();
    }
}
