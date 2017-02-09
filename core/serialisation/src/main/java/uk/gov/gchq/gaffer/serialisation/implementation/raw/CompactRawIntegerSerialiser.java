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

package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

/**
 * Serialises integers using a variable-length scheme that means smaller integers get serialised into a smaller
 * number of bytes. For example, integers i which are between -112 and 127 inclusive are serialised into one byte. Very
 * large integers may be serialised into 5 bytes. This is particularly well suited to serialising count properties in
 * power-law graphs where the majority of counts will be very small.
 * Note that {@link CompactRawLongSerialiser} does not use any more bytes than this serialiser if it is
 * serialising a long value that is less than or equal to <code>Integer.MAX_VALUE</code> and greater than or
 * equal to <code>Integer.MIN_VALUE</code>. This means that, in terms of serialised size, there is no benefit to
 * using an integer instead of a long.
 */
public class CompactRawIntegerSerialiser implements Serialisation<Integer> {

    private static final long serialVersionUID = -2874472098583724627L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Integer.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Integer i) throws SerialisationException {
        return CompactRawSerialisationUtils.writeLong(i);
    }

    @Override
    public Integer deserialise(final byte[] bytes) throws SerialisationException {
        final long result = CompactRawSerialisationUtils.readLong(bytes);
        if ((result > Integer.MAX_VALUE) || (result < Integer.MIN_VALUE)) {
            throw new SerialisationException("Value too long to fit in integer");
        }
        return (int) result;
    }

    @Override
    public Integer deserialiseEmptyBytes() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
