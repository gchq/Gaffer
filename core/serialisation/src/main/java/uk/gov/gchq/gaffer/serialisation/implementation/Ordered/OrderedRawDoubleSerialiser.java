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
 * OrderedRawDoubleSerialiser encodes/decodes a Double to/from a byte array.
 */
public class OrderedRawDoubleSerialiser extends AbstractOrderedSerialiser<Double> {

    private static final long serialVersionUID = -3107286768426662668L;
    OrderedRawLongSerialiser uLongSerialiser = new OrderedRawLongSerialiser();

    @Override
    public byte[] serialise(final Double d) {
        long l = Double.doubleToRawLongBits(d);
        if (l < 0) {
            l = ~l;
        } else {
            l = l ^ 0x8000000000000000L;
        }

        return uLongSerialiser.serialise(l);
    }

    @Override
    public Double deserialise(final byte[] b) {
        return super.deserialise(b);
    }

    @Override
    protected Double deserialiseUnchecked(final byte[] data, final int offset, final int len) {
        long l = uLongSerialiser.deserialiseUnchecked(data, offset, len);
        if (l < 0) {
            l = l ^ 0x8000000000000000L;
        } else {
            l = ~l;
        }
        return Double.longBitsToDouble(l);
    }

    public boolean canHandle(final Class clazz) {
        return Double.class.equals(clazz);
    }
}
