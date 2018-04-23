/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.util;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Delegate for the storage of {@link uk.gov.gchq.gaffer.serialisation.implementation.MultiSerialiser}
 */
public class MultiSerialiserStorage {
    private Map<Byte, Class<? extends ToBytesSerialiser>> keyToSerialiser = new HashMap<>();
    private Map<Byte, Class> keyToValueMap = new HashMap<>();
    private Map<Class, Byte> valueToKeyMap = new HashMap<>();
    private boolean consistent = true;
    private boolean preservesObjectOrdering = true;
    private byte savedSerialiserEncoding = ((byte) 0);

    public void put(Class<? extends ToBytesSerialiser> serialiserClass, Class supportedClass) throws GafferCheckedException {
        put(getSerialiserEncoding(), serialiserClass, supportedClass);
    }

    public void put(byte serialiserEncoding, Class<? extends ToBytesSerialiser> serialiserClass, Class supportedClass) throws GafferCheckedException {
        ToBytesSerialiser toBytesSerialiser = getSerialiser(serialiserClass, supportedClass);

        consistent = continuesToBeConsistant(toBytesSerialiser);
        preservesObjectOrdering = continuesToPreserveOrdering(toBytesSerialiser);

        keyToSerialiser.put(serialiserEncoding, serialiserClass);
        keyToValueMap.put(serialiserEncoding, supportedClass);
        valueToKeyMap.put(supportedClass, serialiserEncoding);
    }

    private ToBytesSerialiser getSerialiser(final Class<? extends ToBytesSerialiser> serialiserClass, final Class supportedClass) throws GafferCheckedException {
        ToBytesSerialiser toBytesSerialiser;
        try {
            toBytesSerialiser = serialiserClass.newInstance();
        } catch (Exception e) {
            throw new GafferCheckedException(String.format("Unable to Instantiate serialiser to validate that %s can handle %s", serialiserClass.getSimpleName(), supportedClass.getSimpleName()), e);
        }
        if (!toBytesSerialiser.canHandle(supportedClass)) {
            throw new GafferCheckedException(String.format("%s does not handle %s", toBytesSerialiser.getClass(), supportedClass));
        }
        return toBytesSerialiser;
    }

    private byte getSerialiserEncoding() {
        Set<Byte> integers = keyToSerialiser.keySet();
        while (integers.contains(savedSerialiserEncoding)) {
            savedSerialiserEncoding++;
        }
        return savedSerialiserEncoding;
    }

    public ToBytesSerialiser getSerialiserFromKey(final byte key) throws GafferCheckedException {
        Class<? extends ToBytesSerialiser> serialiserClass = keyToSerialiser.get(key);
        try {
            return (null == serialiserClass) ? null : serialiserClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GafferCheckedException(String.format("Unable to Instantiate serialiser, this means that serialiser was not checked when added to the storage: %s", serialiserClass));
        }
    }

    public ToBytesSerialiser getSerialiserFromValue(final Object object) throws GafferCheckedException {
        if (null == object) {
            return null;
        }
        Class<? extends ToBytesSerialiser> serialiserClass = keyToSerialiser.get(valueToKeyMap.get(object.getClass()));
        try {
            return (null == serialiserClass) ? null : serialiserClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GafferCheckedException(String.format("Unable to Instantiate serialiser, this means that serialiser was not checked within the put method: %s", serialiserClass));
        }
    }

    public Byte getKeyFromSerialiser(final Class<? extends ToBytesSerialiser> serialiser) {
        for (Entry<Byte, Class<? extends ToBytesSerialiser>> entry : keyToSerialiser.entrySet()) {
            if (entry.getValue().equals(serialiser)) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * @param handleClass
     * @return {@link ToBytesSerialiser#canHandle(Class)}
     * @throws GafferCheckedException
     * @see ToBytesSerialiser
     */
    public boolean canHandle(final Class handleClass) throws GafferCheckedException {
        boolean rtn = false;
        for (Class<? extends ToBytesSerialiser> serialiserClass : keyToSerialiser.values()) {
            ToBytesSerialiser toBytesSerialiser;
            try {
                toBytesSerialiser = serialiserClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new GafferCheckedException(String.format("Unable to Instantiate serialiser, this means that serialiser was not checked within the put method: %s", serialiserClass));
            }
            rtn = toBytesSerialiser.canHandle(handleClass);
            if (rtn) {
                break;
            }
        }
        return rtn;
    }

    private boolean continuesToPreserveOrdering(final ToBytesSerialiser toBytesSerialiser) {
        return preservesObjectOrdering && toBytesSerialiser.preservesObjectOrdering();
    }

    private boolean continuesToBeConsistant(final ToBytesSerialiser toBytesSerialiser) {
        return consistent && toBytesSerialiser.isConsistent();
    }

    /**
     * @return {@link ToBytesSerialiser#preservesObjectOrdering()}
     */
    public boolean preservesObjectOrdering() {
        return preservesObjectOrdering;
    }

    /**
     * @return {@link ToBytesSerialiser#isConsistent()}
     */
    public boolean isConsistent() {
        return consistent;
    }
}
