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

import java.util.Date;

/**
 * For new properties use {@link uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDateSerialiser}.
 * Serialises {@link Date}s to an array of bytes of length 8 by directly converting the underlying long to a
 * byte array. This serialiser preserves ordering, i.e. if date1 is less than date2
 * then serialise(date1) is less than serialise(date2)
 * where the byte arrays are compared one byte at a time starting with the first.
 *
 * @deprecated this is unable to preserve object ordering.
 * @see uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDateSerialiser
 */
@Deprecated
public class RawDateSerialiser implements ToBytesSerialiser<Date> {
    private static final long serialVersionUID = -1470994471883677977L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Date.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Date date) throws SerialisationException {
        final byte[] out = new byte[8];
        final long value = date.getTime();
        // NB Serialise high-order bits first
        out[0] = (byte) ((value >> 56) & 255);
        out[1] = (byte) ((value >> 48) & 255);
        out[2] = (byte) ((value >> 40) & 255);
        out[3] = (byte) ((value >> 32) & 255);
        out[4] = (byte) ((value >> 24) & 255);
        out[5] = (byte) ((value >> 16) & 255);
        out[6] = (byte) ((value >> 8) & 255);
        out[7] = (byte) (value & 255);
        return out;
    }

    @Override
    public Date deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        int carriage = offset;
        final long value = (allBytes[carriage++] & 255L) << 56
                | (allBytes[carriage++] & 255L) << 48
                | (allBytes[carriage++] & 255L) << 40
                | (allBytes[carriage++] & 255L) << 32
                | (allBytes[carriage++] & 255L) << 24
                | (allBytes[carriage++] & 255L) << 16
                | (allBytes[carriage++] & 255L) << 8
                | allBytes[carriage] & 255L;
        return new Date(value);
    }

    @Override
    public Date deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public Date deserialiseEmpty() {
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
