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
 * An {@code OrderedFloatSerialser} serialises a {@link Float} to
 * an array of bytes by directly converting the float to a raw int and
 * then converting this to a byte array.
 * This serialiser preserves ordering.
 */
public class OrderedFloatSerialiser implements ToBytesSerialiser<Float> {

    private static final long serialVersionUID = 6829577492677279853L;
    private static final OrderedIntegerSerialiser INTEGER_SERIALISER = new OrderedIntegerSerialiser();

    @Override
    public byte[] serialise(final Float object) {
        int i = Float.floatToRawIntBits(object);
        if (i < 0) {
            i = ~i;
        } else {
            i = i ^ 0x80000000;
        }
        return INTEGER_SERIALISER.serialise(i);
    }

    @Override
    public Float deserialise(final byte[] bytes) throws SerialisationException {
        int i = INTEGER_SERIALISER.deserialise(bytes);
        if (i < 0) {
            i = i ^ 0x80000000;
        } else {
            i = ~i;
        }
        return Float.intBitsToFloat(i);
    }

    @Override
    public Float deserialiseEmpty() {
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
        return Float.class.equals(clazz);
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return OrderedFloatSerialiser.class.getName().hashCode();
    }
}
