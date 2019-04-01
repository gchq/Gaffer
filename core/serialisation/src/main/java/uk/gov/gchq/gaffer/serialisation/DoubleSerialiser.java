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
package uk.gov.gchq.gaffer.serialisation;


import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;

/**
 * @deprecated this is not very efficient and should only be used for compatibility
 * reasons. For new properties use {@link uk.gov.gchq.gaffer.serialisation.implementation.raw.RawDoubleSerialiser}
 * instead.
 */
@Deprecated
public class DoubleSerialiser extends ToBytesViaStringDeserialiser<Double> {
    private static final long serialVersionUID = 5647756843689779437L;

    public DoubleSerialiser() {
        super(CommonConstants.ISO_8859_1_ENCODING);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Double.class.equals(clazz);
    }

    @Override
    protected String serialiseToString(final Double object) throws SerialisationException {
        return object.toString();
    }

    @Override
    public Double deserialiseString(final String value) throws SerialisationException {
        try {
            return Double.parseDouble(value);
        } catch (final NumberFormatException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Double deserialiseEmpty() {
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
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    public int hashCode() {
        return DoubleSerialiser.class.getName().hashCode();
    }
}
