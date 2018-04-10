/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * This class is used to serialise and deserialise byte arrays.
 */
public class BytesSerialiser implements ToBytesSerialiser<byte[]> {
    private static final long serialVersionUID = -7718650654168267452L;

    @Override
    public boolean canHandle(final Class clazz) {
        return byte[].class.equals(clazz) || Byte[].class.equals(clazz);
    }

    @Override
    public byte[] serialise(final byte[] value) throws SerialisationException {
        return value;
    }

    @Override
    public byte[] deserialise(final byte[] bytes) throws SerialisationException {
        return bytes;
    }

    @Override
    public byte[] deserialiseEmpty() {
        return new byte[0];
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }
}
