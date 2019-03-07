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

package uk.gov.gchq.gaffer.serialisation.implementation.ordered;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * An {@code OrderedLongSerialser} serialises a {@link Long} to
 * an array of bytes. This serialiser preserves ordering.
 * The serialser sorts Long.MIN_VALUE first and Long.MAX_VALUE last.
 */
public class OrderedLongSerialiser implements ToBytesSerialiser<Long> {

    private static final long serialVersionUID = -8948380879926929233L;

    @Override
    public byte[] serialise(final Long object) {
        final Long signedL = object ^ 0x8000000000000000L;
        int shift = 56;
        int index;
        int prefix = signedL < 0 ? 0xff : 0x00;

        for (index = 0; index < 8; index++) {
            if (((signedL >> shift) & 0xff) != prefix) {
                break;
            }

            shift -= 8;
        }

        byte[] ret = new byte[9 - index];
        ret[0] = (byte) (8 - index);
        for (index = 1; index < ret.length; index++) {
            ret[index] = (byte) (signedL >> shift);
            shift -= 8;
        }

        if (signedL < 0) {
            ret[0] = (byte) (16 - ret[0]);
        }

        return ret;
    }

    @Override
    public Long deserialise(final byte[] bytes) throws SerialisationException {

        long l = 0;
        int shift = 0;

        if (bytes[0] < 0 || bytes[0] > 16) {
            throw new SerialisationException("Unexpected length " + (0xff & bytes[0]));
        }

        for (int i = bytes.length - 1; i >= 0 + 1; i--) {
            l += (bytes[i] & 0xffL) << shift;
            shift += 8;
        }

        if (bytes[0] > 8) {
            l |= -1L << ((16 - bytes[0]) << 3);
        }

        return l ^ 0x8000000000000000L;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Long.class.equals(clazz);
    }

    @Override
    public Long deserialiseEmpty() {
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
        return OrderedLongSerialiser.class.getName().hashCode();
    }
}
