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
package uk.gov.gchq.gaffer.serialisation.implementation;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawDateSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawDoubleSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawFloatSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawLongSerialiser;
import java.util.Collections;
import java.util.List;

/**
 * A <code>SerialisationFactory</code> holds a list of core serialisers and
 * is design to provide compatible serialisers for given object classes.
 */
public class SerialisationFactory {

    private final List<Serialiser> serialisers;
    private static final Serialiser LAST_RESORT_FINALISER = new JavaSerialiser();
    private static final Serialiser[] SERIALISERS = new Serialiser[]{
            new StringSerialiser(),
            new BytesSerialiser(),
            new CompactRawIntegerSerialiser(),
            new RawIntegerSerialiser(),
            new CompactRawLongSerialiser(),
            new RawLongSerialiser(),
            new BooleanSerialiser(),
            new RawDateSerialiser(),
            new RawDoubleSerialiser(),
            new RawFloatSerialiser(),
            new TreeSetStringSerialiser(),
    };

    public SerialisationFactory() {
        this.serialisers = Lists.newArrayList(SERIALISERS);
    }

    /**
     * Constructor.
     *
     * @param serialisers a list of Serialisers.
     */
    public SerialisationFactory(final Serialiser... serialisers) {
        this.serialisers = Lists.newArrayList(serialisers);
    }

    public List<Serialiser> getSerialisers() {
        return serialisers;
    }

    /**
     * Adds a list of {@link Serialiser} to be used within the SerialisationFactory.
     *
     * @param newSerialisers a list of Serialisers.
     */
    public void addSerialisers(final Serialiser... newSerialisers) {
        Collections.addAll(serialisers, newSerialisers);
    }

    /**
     * @param objClass the class of an object to be serialised.
     * @return a compatible serialiser.
     * @throws IllegalArgumentException if the object class parameter is null or
     *                                  no compatible serialiser could be found
     */
    public Serialiser getSerialiser(final Class<?> objClass) {
        return getSerialiser(objClass, false);
    }

    /**
     * @param objClass      the class of an object to be serialised.
     * @param preserveOrder if true then the returned serialiser should preserve order
     * @return a compatible serialiser.
     * @throws IllegalArgumentException if the object class parameter is null or
     *                                  no compatible serialiser could be found
     */
    public Serialiser getSerialiser(final Class<?> objClass, final boolean preserveOrder) {
        if (null == objClass) {
            throw new IllegalArgumentException("Object class for serialising is required");
        }

        for (final Serialiser serialiser : serialisers) {
            if (canSerialiseClass(objClass, preserveOrder, serialiser)) {
                return serialiser;
            }
        }
        if (canSerialiseClass(objClass, preserveOrder, LAST_RESORT_FINALISER)) {
            return LAST_RESORT_FINALISER;
        }

        throw new IllegalArgumentException("No serialiser found for object class: " + objClass);
    }

    /**
     * Checks the given serialiser is able to serialise the given class
     *
     * @param objClass      the class of an object to be serialised.
     * @param preserveOrder if true then the returned serialiser should preserve the order
     * @param serialiser    a compatible serialiser
     * @return <CODE> true </CODE> if serialiser can serialise the class, <CODE> false </CODE> otherwise
     */
    private boolean canSerialiseClass(final Class<?> objClass, final boolean preserveOrder, final Serialiser serialiser) {
        return serialiser.canHandle(objClass) && (!preserveOrder || serialiser.preservesObjectOrdering());
    }
}
