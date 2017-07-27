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

public class OrderedDoubleSerialiser extends DelegateSerialiser<Double, Long> {

    private static final long serialVersionUID = -4750738170126596560L;
    private static final OrderedLongSerialiser LONG_SERIALISER = new OrderedLongSerialiser();

    public OrderedDoubleSerialiser() {
        super(LONG_SERIALISER);
    }

    @Override
    public Double fromDelegateType(final Long object) {
        long l = object;
        if (l < 0) {
            l = l ^ 0x8000000000000000L;
        } else {
            l = ~l;
        }
        return Double.longBitsToDouble(l);
    }

    @Override
    public Long toDelegateType(final Double object) {
        long l = Double.doubleToRawLongBits(object);
        if (l < 0) {
            l = ~l;
        } else {
            l = l ^ 0x8000000000000000L;
        }
        return l;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Double.class.equals(clazz);
    }
}
