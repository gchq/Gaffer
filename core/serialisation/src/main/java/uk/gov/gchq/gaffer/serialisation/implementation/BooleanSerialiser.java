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
package uk.gov.gchq.gaffer.serialisation.implementation;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

/**
 * This class is used to serialise and deserialise a boolean value
 */
public class BooleanSerialiser implements Serialisation<Boolean> {

    private static final long serialVersionUID = -3964992157560886710L;
    private static final byte FALSE = (byte) 0;
    private static final byte TRUE = (byte) 1;

    public byte[] serialise(final Boolean value) throws SerialisationException {
        return new byte[]{Boolean.TRUE.equals(value) ? TRUE : FALSE};
    }

    public Boolean deserialise(final byte[] bytes) throws SerialisationException {
        return bytes.length == 1 && TRUE == bytes[0];
    }

    @Override
    public Boolean deserialiseEmptyBytes() {
        return Boolean.FALSE;
    }

    public <T> T deserialise(final byte[] bytes, final Class<T> clazz) throws SerialisationException {
        return clazz.cast(bytes.length == 1 && TRUE == bytes[0]);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Boolean.class.isAssignableFrom(clazz);
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }
}
