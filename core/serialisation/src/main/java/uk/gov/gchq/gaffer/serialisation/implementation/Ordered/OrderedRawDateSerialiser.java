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

package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import uk.gov.gchq.gaffer.serialisation.AbstractOrderedSerialiser;
import java.util.Date;

/**
 * OrderedRawDateSerialiser encodes/decodes a Date to/from a byte array.
 */
public class OrderedRawDateSerialiser extends AbstractOrderedSerialiser<Date> {

    private static final long serialVersionUID = -639298107482091043L;
    private OrderedRawLongSerialiser uLongEncoder = new OrderedRawLongSerialiser();

    @Override
    public byte[] serialise(final Date data) {
        return uLongEncoder.serialise(data.getTime());
    }

    @Override
    public Date deserialise(final byte[] bytes) {
        return super.deserialise(bytes);
    }

    @Override
    protected Date deserialiseUnchecked(final byte[] data, final int offset, final int len) {
        return new Date(uLongEncoder.deserialiseUnchecked(data, offset, len));
    }

    public boolean canHandle(final Class clazz) {
        return Date.class.equals(clazz);
    }
}
