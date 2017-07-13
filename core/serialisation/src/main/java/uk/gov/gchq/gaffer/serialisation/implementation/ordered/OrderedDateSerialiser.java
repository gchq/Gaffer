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

package uk.gov.gchq.gaffer.serialisation.implementation.ordered;

import uk.gov.gchq.gaffer.serialisation.DelegateSerialiser;
import java.util.Date;

public class OrderedDateSerialiser extends DelegateSerialiser<Date, Long> {

    private static final long serialVersionUID = 6636121009320739764L;
    private static final OrderedLongSerialiser LONG_SERIALISER = new OrderedLongSerialiser();

    public OrderedDateSerialiser() {
        super(LONG_SERIALISER);
    }

    @Override
    public Date fromDelegateType(final Long object) {
        return new Date(object);
    }

    @Override
    public Long toDelegateType(final Date object) {
        return object.getTime();
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Date.class.equals(clazz);
    }
}
