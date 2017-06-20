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
 * OrderedRawFloatSerialiser encodes/decodes a Float to/from a byte array.
 */
public class OrderedRawFloatSerialiser extends AbstractOrderedSerialiser<Float> {

    private static final long serialVersionUID = -5876936326255922732L;
    OrderedRawIntegerSerialiser uIntSerialiser = new OrderedRawIntegerSerialiser();

    @Override
    public byte[] serialise(final Float f) {
        int i = Float.floatToRawIntBits(f);
        if (i < 0) {
            i = ~i;
        } else {
            i = i ^ 0x80000000;
        }

        return uIntSerialiser.serialise(i);
    }

    @Override
    public Float deserialise(final byte[] b) {
        return super.deserialise(b);
    }

    @Override
    protected Float deserialiseUnchecked(final byte[] data, final int offset, final int len) {
        int i = uIntSerialiser.deserialiseUnchecked(data, offset, len);
        if (i < 0) {
            i = i ^ 0x80000000;
        } else {
            i = ~i;
        }

        return Float.intBitsToFloat(i);
    }

    public boolean canHandle(final Class clazz) {
        return Float.class.equals(clazz);
    }
}
