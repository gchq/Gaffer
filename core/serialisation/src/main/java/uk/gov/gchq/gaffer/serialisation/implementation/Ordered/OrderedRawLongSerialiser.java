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
 * OrderedRawLongSerialiser encodes/decodes an signed Long to/from a byte array.
 */
public class OrderedRawLongSerialiser extends AbstractOrderedSerialiser<Long> {

    private static final long serialVersionUID = 4023688768692449245L;

    @Override
    public byte[] serialise(final Long l) {
        final Long signedL = l ^ 0x8000000000000000L;
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
    protected Long deserialiseUnchecked(final byte[] data, final int offset, final int len) {

        long l = 0;
        int shift = 0;

        if (data[offset] < 0 || data[offset] > 16) {
            throw new IllegalArgumentException("Unexpected length " + (0xff & data[offset]));
        }

        for (int i = (offset + len) - 1; i >= offset + 1; i--) {
            l += (data[i] & 0xffL) << shift;
            shift += 8;
        }

        if (data[offset] > 8) {
            l |= -1L << ((16 - data[offset]) << 3);
        }

        return l ^ 0x8000000000000000L;
    }

    @Override
    public Long deserialise(final byte[] b) {
        return super.deserialise(b);
    }

    public boolean canHandle(final Class clazz) {
        return Long.class.equals(clazz);
    }
}
