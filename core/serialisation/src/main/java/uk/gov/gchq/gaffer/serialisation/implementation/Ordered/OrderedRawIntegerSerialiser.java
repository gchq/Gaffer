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

package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import uk.gov.gchq.gaffer.serialisation.AbstractOrderedSerialiser;

/**
 * OrderedRawIntegerSerialiser encodes/decodes a signed Integer to/from a byte array.
 * This sorts Integer.MIN_VALUE first and INTEGER.MAX_VALUE last.
 */
public class OrderedRawIntegerSerialiser extends AbstractOrderedSerialiser<Integer> {

    private static final long serialVersionUID = 1950758281685062043L;

    @Override
    public byte[] serialise(final Integer i) {
        final Integer signedI = i ^ 0x80000000;
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
    public Integer deserialise(final byte[] b) {
        return super.deserialise(b);
    }

    @Override
    protected Integer deserialiseUnchecked(final byte[] data, final int offset, final int len) {
        if (data[offset] >= 0 && data[offset] <= 8) {
            int i = 0;
            int shift = 0;

            for (int idx = offset + len - 1; idx >= offset + 1; --idx) {
                i = (int) ((long) i + (((long) data[idx] & 255L) << shift));
                shift += 8;
            }

            if (data[offset] > 4) {
                i |= -1 << (8 - data[offset] << 3);
            }

            return Integer.valueOf(i) ^ 0x80000000;
        } else {
            throw new IllegalArgumentException("Unexpected length " + (255 & data[offset]));
        }
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Long.class.equals(clazz);
    }
}
