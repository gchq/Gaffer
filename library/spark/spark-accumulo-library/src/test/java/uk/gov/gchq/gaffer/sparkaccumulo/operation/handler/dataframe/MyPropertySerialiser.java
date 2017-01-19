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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;

/**
 * A serialiser for {@link MyProperty}.
 */
public class MyPropertySerialiser implements Serialisation {
    private final CompactRawIntegerSerialiser integerSerialiser = new CompactRawIntegerSerialiser();

    @Override
    public boolean canHandle(Class clazz) {
        return MyProperty.class.equals(clazz);
    }

    @Override
    public byte[] serialise(Object object) throws SerialisationException {
        return integerSerialiser.serialise(((MyProperty) object).getA());
    }

    @Override
    public Object deserialise(byte[] bytes) throws SerialisationException {
        return new MyProperty(integerSerialiser.deserialise(bytes));
    }

    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }

    @Override
    public Object deserialiseEmptyBytes() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
