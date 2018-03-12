/*
 * Copyright 2017-2018 Crown Copyright
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
 * An {@code OrderedIntegerSerialser} serialises a {@link Integer} to
 * an array of bytes. This serialiser preserves ordering.
 * The serialser sorts Integer.MIN_VALUE first and Integer.MAX_VALUE last.
 */
public class OrderedIntegerSerialiser implements ToBytesSerialiser<Integer> {

    private static final long serialVersionUID = 5671653945533196758L;

    @Override
    public byte[] serialise(final Integer object) {
        final Integer signedI = object ^ 0x80000000;
        int shift = 56;
        int prefix = (signedI.intValue()) < 0 ? 255 : 0;

        int index;
        for (index = 0; index < 4 && (signedI.intValue() >> shift & 255) == prefix; ++index) {
            shift -= 8;
        }

        byte[] ret = new byte[5 - index];
        ret[0] = (byte) (4 - index);

        for (index = 1; index < ret.length; ++index) {
            ret[index] = (byte) (signedI.intValue() >> shift);
            shift -= 8;
        }

        if (signedI.intValue() < 0) {
            ret[0] = (byte) (8 - ret[0]);
        }
        return ret;
    }

    @Override
    public Integer deserialise(final byte[] bytes) throws SerialisationException {
        if (bytes[0] >= 0 && bytes[0] <= 8) {
            int i = 0;
            int shift = 0;

            for (int idx = bytes.length - 1; idx >= 0 + 1; --idx) {
                i = (int) ((long) i + (((long) bytes[idx] & 255L) << shift));
                shift += 8;
            }

            if (bytes[0] > 4) {
                i |= -1 << (8 - bytes[0] << 3);
            }
            return Integer.valueOf(i) ^ 0x80000000;
        } else {
            throw new SerialisationException("Unexpected length " + (255 & bytes[0]));
        }
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

    @Override
    public boolean canHandle(final Class clazz) {
        return Integer.class.equals(clazz);
    }
}
