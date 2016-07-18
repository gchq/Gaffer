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

package gaffer.serialisation.simple.raw;

import gaffer.exception.SerialisationException;
import gaffer.serialisation.Serialisation;

/**
 * Serialises longs using a variable-length scheme that means smaller longs get serialised into a smaller
 * number of bytes. For example, longs i which are between -112 and 127 inclusive are serialised into one byte. Very
 * large longs may be serialised into 9 bytes. This is particularly well suited to serialising count properties in
 * power-law graphs where the majority of counts will be very small.
 */
public class CompactRawLongSerialiser implements Serialisation {

    private static final long serialVersionUID = 6104372357426908732L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Long.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Object o) throws SerialisationException {
        return CompactRawSerialisationUtils.writeLong((long) o);
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        return CompactRawSerialisationUtils.readLong(bytes);
    }

}
