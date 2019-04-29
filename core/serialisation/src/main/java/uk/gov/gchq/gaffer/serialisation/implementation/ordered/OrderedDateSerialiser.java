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

import java.util.Date;

/**
 * An {@code OrderedDateSerialiser} serialises a {@link Date} to
 * an array of bytes by directly converting the underlying long to a byte array.
 * This serialiser preserves ordering.
 */
public class OrderedDateSerialiser implements ToBytesSerialiser<Date> {

    private static final long serialVersionUID = 6636121009320739764L;
    private static final OrderedLongSerialiser LONG_SERIALISER = new OrderedLongSerialiser();

    @Override
    public byte[] serialise(final Date object) {
        return LONG_SERIALISER.serialise(object.getTime());
    }

    @Override
    public Date deserialise(final byte[] bytes) throws SerialisationException {
        return new Date(LONG_SERIALISER.deserialise(bytes));
    }

    @Override
    public Date deserialiseEmpty() {
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
        return Date.class.equals(clazz);
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return OrderedDateSerialiser.class.getName().hashCode();
    }

}
