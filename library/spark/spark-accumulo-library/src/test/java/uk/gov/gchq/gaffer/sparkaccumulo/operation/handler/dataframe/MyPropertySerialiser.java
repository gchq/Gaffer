/*
 * Copyright 2016-2019 Crown Copyright
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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;

/**
 * A serialiser for {@link MyProperty}.
 */
public class MyPropertySerialiser implements ToBytesSerialiser<MyProperty> {
    private final CompactRawIntegerSerialiser integerSerialiser = new CompactRawIntegerSerialiser();

    @Override
    public boolean canHandle(final Class clazz) {
        return MyProperty.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final MyProperty object) throws SerialisationException {
        return integerSerialiser.serialise(object.getA());
    }

    @Override
    public MyProperty deserialise(final byte[] bytes) throws SerialisationException {
        return new MyProperty(integerSerialiser.deserialise(bytes));
    }

    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }

    @Override
    public MyProperty deserialiseEmpty() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final MyPropertySerialiser serialiser = (MyPropertySerialiser) obj;

        return new EqualsBuilder()
                .append(integerSerialiser, serialiser.integerSerialiser)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(integerSerialiser)
                .toHashCode();
    }
}
