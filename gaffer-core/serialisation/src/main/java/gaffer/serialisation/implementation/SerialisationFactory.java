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
package gaffer.serialisation.implementation;

import gaffer.serialisation.Serialisation;
import gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;

/**
 * A <code>SerialisationFactory</code> holds a list of core serialisers and
 * is design to provide compatible serialisers for given object classes.
 */
public class SerialisationFactory {
    private static final Serialisation[] SERIALISERS = new Serialisation[]{
            new BooleanSerialiser(),
            new DateSerialiser(),
            new DoubleSerialiser(),
            new FloatSerialiser(),
            new CompactRawIntegerSerialiser(),
            new CompactRawLongSerialiser(),
            new StringSerialiser(),
            new TreeSetStringSerialiser(),
            new JavaSerialiser()
    };

    /**
     * @param objClass the class of an object to be serialised.
     * @return a compatible serialiser.
     * @throws IllegalArgumentException if the object class parameter is null or
     *                                  no compatible serialiser could be found
     */
    public Serialisation getSerialiser(final Class<?> objClass) {
        if (null == objClass) {
            throw new IllegalArgumentException("Object class for serialising is required");
        }

        for (Serialisation serialiser : SERIALISERS) {
            if (serialiser.canHandle(objClass)) {
                return serialiser;
            }
        }

        throw new IllegalArgumentException("No serialiser found for object class: " + objClass);
    }
}
