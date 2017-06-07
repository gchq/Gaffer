/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore.serialisation;

import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.SerialisationFactory;

/**
 * A <code>ParquetSerialisationFactory</code> holds a list of core serialisers and
 * is design to provide compatible serialisers for given object classes.
 */
public class ParquetSerialisationFactory extends SerialisationFactory {
    private static final Serialiser[] SERIALISERS = new Serialiser[] {
            new StringParquetSerialiser(),
            new ByteParquetSerialiser(),
            new IntegerParquetSerialiser(),
            new LongParquetSerialiser(),
            new BooleanParquetSerialiser(),
            new DateParquetSerialiser(),
            new DoubleParquetSerialiser(),
            new FloatParquetSerialiser(),
            new InLineHyperLogLogPlusParquetSerialiser(),
            new ShortParquetSerialiser(),
            new TypeValueParquetSerialiser(),
            new JavaSerialiser()
    };

    /**
     * @param objClass      the class of an object to be serialised.
     * @param preserveOrder if true then the returned serialiser should preserve order
     * @return a compatible serialiser.
     * @throws IllegalArgumentException if the object class parameter is null or
     *                                  no compatible serialiser could be found
     */
    @Override
    public Serialiser getSerialiser(final Class<?> objClass, final boolean preserveOrder) {
        if (null == objClass) {
            throw new IllegalArgumentException("Object class for serialising is required");
        }

        for (final Serialiser serialiser : SERIALISERS) {
            if (serialiser.canHandle(objClass) && (!preserveOrder || (serialiser.preservesObjectOrdering()))) {
                return serialiser;
            }
        }
        throw new IllegalArgumentException("No serialiser found for object class: " + objClass);
    }
}
