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
package uk.gov.gchq.gaffer.store;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.serialisation.FreqMapSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.TypeSubTypeValueSerialiser;
import uk.gov.gchq.gaffer.serialisation.TypeValueSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.BooleanSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.BytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.TreeSetStringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDateSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDoubleSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedFloatSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedLongSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;

import java.util.List;

/**
 * A {@code SerialisationFactory} holds a list of core serialisers and
 * is design to provide compatible serialisers for given object classes.
 */
public class SerialisationFactory {

    private final List<Serialiser> serialisers;
    private static final Serialiser LAST_RESORT_FINALISER = new JavaSerialiser();
    private static final Serialiser[] CORE_SERIALISERS = new Serialiser[]{
            new OrderedLongSerialiser(),
            new OrderedDateSerialiser(),
            new OrderedIntegerSerialiser(),
            new OrderedDoubleSerialiser(),
            new OrderedFloatSerialiser(),
            new StringSerialiser(),
            new BytesSerialiser(),
            new CompactRawIntegerSerialiser(),
            new CompactRawLongSerialiser(),
            new BooleanSerialiser(),
            new TreeSetStringSerialiser(),
            new TypeValueSerialiser(),
            new TypeSubTypeValueSerialiser(),
            new FreqMapSerialiser()
    };

    public SerialisationFactory() {
        this.serialisers = Lists.newArrayList(CORE_SERIALISERS);
    }

    /**
     * Constructor.
     *
     * @param serialisers a list of Serialisers.
     */
    public SerialisationFactory(final Serialiser... serialisers) {
        this.serialisers = Lists.newArrayList(serialisers);
    }

    /**
     * @return List of set Serialisers.
     */
    public List<Serialiser> getSerialisers() {
        return serialisers;
    }

    /**
     * Adds a list of {@link Serialiser} to be used within the SerialisationFactory.
     *
     * @param newSerialisers a list of Serialisers.
     */
    public void addSerialisers(final Serialiser... newSerialisers) {
        if (null != newSerialisers) {
            for (final Serialiser newSerialiser : newSerialisers) {
                if (!serialiserAlreadyinList(newSerialiser)) {
                    serialisers.add(newSerialiser);
                }
            }
        }
    }

    /**
     * @param objClass the class of an object to be serialised.
     * @return a compatible serialiser.
     * @throws IllegalArgumentException if the object class parameter is null or
     *                                  no compatible serialiser could be found.
     */
    public Serialiser getSerialiser(final Class<?> objClass) {
        return getSerialiser(objClass, false, false);
    }

    /**
     * @param objClass      the class of an object to be serialised.
     * @param preserveOrder if true then the returned serialiser should preserve order.
     * @param consistentSerialiser if true then the returned serialiser should be consistent
     * @return a compatible serialiser.
     * @throws IllegalArgumentException if the object class parameter is null or
     *                                  no compatible serialiser could be found.
     */
    public Serialiser getSerialiser(final Class<?> objClass, final boolean preserveOrder, final boolean consistentSerialiser) {
        if (null == objClass) {
            throw new IllegalArgumentException("Object class for serialising is required");
        }

        for (final Serialiser serialiser : serialisers) {
            if (canSerialiseClass(objClass, preserveOrder, serialiser) && (!consistentSerialiser || serialiser.isConsistent())) {
                return serialiser;
            }
        }
        if (canSerialiseClass(objClass, preserveOrder, LAST_RESORT_FINALISER)) {
            return LAST_RESORT_FINALISER;
        }

        throw new IllegalArgumentException("No serialiser found for object class: " + objClass);
    }

    /**
     * Checks the given serialiser is able to serialise the given class.
     *
     * @param objClass      the class of an object to be serialised.
     * @param preserveOrder if true then the returned serialiser should preserve the order.
     * @param serialiser    a compatible serialiser.
     * @return {@code true } if serialiser can serialise the class, {@code false } otherwise.
     */
    private boolean canSerialiseClass(final Class<?> objClass, final boolean preserveOrder, final Serialiser serialiser) {
        return serialiser.canHandle(objClass) && (!preserveOrder || serialiser.preservesObjectOrdering());
    }

    /**
     * Checks if the given serialiser is already in the serialisers list.
     *
     * @param newSerialiser new serialiser to be added to serialisers.
     * @return {@code true } if given serialiser is already present in serialisers, {@code false } otherwise.
     */
    private boolean serialiserAlreadyinList(final Serialiser newSerialiser) {
        for (final Serialiser serialiser : serialisers) {
            if (serialiser.getClass().equals(newSerialiser.getClass())) {
                return true;
            }
        }
        return false;
    }

}
