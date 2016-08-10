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
package gaffer.serialisation.implementation.raw;


import gaffer.exception.SerialisationException;
import gaffer.serialisation.Serialisation;
import java.util.Date;

public class RawDateSerialiser implements Serialisation {
    private static final long serialVersionUID = -3585678555520167582L;
    private final RawLongSerialiser longSerialiser = new RawLongSerialiser();

    @Override
    public boolean canHandle(final Class clazz) {
        return Date.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        return longSerialiser.serialise(((Date) object).getTime());
    }

    @Override
    public Date deserialise(final byte[] bytes) throws SerialisationException {
        return new Date(longSerialiser.deserialise(bytes));
    }

    @Override
    public boolean isByteOrderPreserved() {
        return true;
    }
}
