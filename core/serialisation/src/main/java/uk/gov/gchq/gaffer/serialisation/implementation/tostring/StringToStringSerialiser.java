/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation.tostring;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToStringSerialiser;

/**
 * A {@code StringToStringSerialiser} serialises a {@link String} to
 * a {@link String} without performing any additional changes to the original object.
 */
public class StringToStringSerialiser implements ToStringSerialiser<String> {

    private static final long serialVersionUID = -3000859022586008228L;

    /**
     * Check whether the serialiser can serialise a particular class.
     *
     * @param clazz the object class to serialise
     * @return boolean true if it can be handled
     */
    @Override
    public boolean canHandle(final Class clazz) {
        return String.class.equals(clazz);
    }

    /**
     * Serialise some object and returns the String of the serialised form.
     *
     * @param object the object to be serialised
     * @return String the serialised String
     * @throws SerialisationException if the object fails to serialise
     */
    @Override
    public String serialise(final String object) throws SerialisationException {
        return object;
    }

    /**
     * Deserialise an String into the original object.
     *
     * @param s the String to deserialise
     * @return INPUT the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     */
    @Override
    public String deserialise(final String s) throws SerialisationException {
        return s;
    }

    /**
     * Indicates whether the serialisation process preserves the ordering of the INPUT,
     * i.e. if x and y are objects of class INPUT, and x is less than y, then this method should
     * return true if the serialised form of x is guaranteed to be less than the serialised form
     * of y (using the standard ordering of String).
     * If INPUT is not Comparable then this test makes no sense and false should be returned.
     *
     * @return true if the serialisation will preserve the order of the INPUT, otherwise false.
     */
    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }
}
