/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesViaStringDeserialiser;

/**
 * A {@code StringSerialiser} is used to serialise {@link String}s.
 */
public class StringSerialiser extends ToBytesViaStringDeserialiser<String> {

    private static final long serialVersionUID = 5647756843689779437L;

    public StringSerialiser() {
        super(CommonConstants.UTF_8);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return String.class.equals(clazz);
    }

    @Override
    protected String serialiseToString(final String object) throws SerialisationException {
        return object;
    }

    @Override
    protected String deserialiseString(final String value) throws SerialisationException {
        return value;
    }

    @Override
    public String deserialiseEmpty() {
        return "";
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }
}
