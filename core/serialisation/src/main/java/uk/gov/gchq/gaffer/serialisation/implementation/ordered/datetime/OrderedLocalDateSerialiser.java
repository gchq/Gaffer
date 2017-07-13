/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.implementation.ordered.datetime;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedLongSerialiser;
import java.time.LocalDate;

public class OrderedLocalDateSerialiser implements ToBytesSerialiser<LocalDate> {

    private static final long serialVersionUID = 6636121009320739764L;
    private static final OrderedLongSerialiser LONG_SERIALISER = new OrderedLongSerialiser();

    @Override
    public byte[] serialise(final LocalDate object) {
        return LONG_SERIALISER.serialise(object.toEpochDay());
    }

    @Override
    public LocalDate deserialise(final byte[] bytes) throws SerialisationException {
        return LocalDate.ofEpochDay(LONG_SERIALISER.deserialise(bytes));
    }

    @Override
    public LocalDate deserialiseEmpty() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return LocalDate.class.equals(clazz);
    }
}
