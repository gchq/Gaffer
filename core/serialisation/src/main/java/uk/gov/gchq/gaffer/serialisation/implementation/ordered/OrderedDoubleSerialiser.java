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
 * An {@code OrderedDoubleSerialser} serialises a {@link Double} to
 * an array of bytes by directly converting the double to a raw long and
 * then converting this to a byte array.
 * This serialiser preserves ordering.
 */
public class OrderedDoubleSerialiser implements ToBytesSerialiser<Double> {

    private static final long serialVersionUID = -4750738170126596560L;
    private static final OrderedLongSerialiser LONG_SERIALISER = new OrderedLongSerialiser();

    @Override
    public byte[] serialise(final Double object) {
        long l = Double.doubleToRawLongBits(object);
        if (l < 0) {
            l = ~l;
        } else {
            l = l ^ 0x8000000000000000L;
        }
        return LONG_SERIALISER.serialise(l);
    }

    @Override
    public Double deserialise(final byte[] bytes) throws SerialisationException {
        long l = LONG_SERIALISER.deserialise(bytes);
        if (l < 0) {
            l = l ^ 0x8000000000000000L;
        } else {
            l = ~l;
        }
        return Double.longBitsToDouble(l);
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
    public boolean canHandle(final Class clazz) {
        return Double.class.equals(clazz);
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return OrderedDoubleSerialiser.class.getName().hashCode();
    }
}
