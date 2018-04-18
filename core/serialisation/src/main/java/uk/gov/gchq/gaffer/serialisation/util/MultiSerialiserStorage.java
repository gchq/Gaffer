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

public class MultiSerialiserStorage {
    private Map<Byte, Class<? extends ToBytesSerialiser>> keyMap = new HashMap<>();
    private Map<Class, Class<? extends ToBytesSerialiser>> classMap = new HashMap<>();
    private boolean consistent = true;
    private boolean preservesObjectOrdering = true;
    private byte savedKey = 0;

    public void put(Class<? extends ToBytesSerialiser> serialiserClass, Class supportedClass) throws GafferCheckedException {
        put(getKey(), serialiserClass, supportedClass);
    }

    public void put(byte key, Class<? extends ToBytesSerialiser> serialiserClass, Class supportedClass) throws GafferCheckedException {
        ToBytesSerialiser toBytesSerialiser;
        try {
            toBytesSerialiser = serialiserClass.newInstance();
        } catch (Exception e) {
            throw new GafferCheckedException(String.format("Unable to Instantiate serialiser to validate that %s can handle %s", serialiserClass.getSimpleName(), supportedClass.getSimpleName()), e);
        }
        if (!toBytesSerialiser.canHandle(supportedClass)) {
            throw new GafferCheckedException(String.format("%s does not handle %s", toBytesSerialiser.getClass(), supportedClass));
        }

        consistent = continuesToBeConsistant(toBytesSerialiser);
        preservesObjectOrdering = continuesToPreserveOrdering(toBytesSerialiser);

        keyMap.put(key, serialiserClass);
        classMap.put(supportedClass, serialiserClass);
    }

    public byte getKey() {
        Set<Byte> integers = keyMap.keySet();
        while (integers.contains(savedKey)) {
            savedKey++;
        }
        return savedKey;
    }

    public ToBytesSerialiser getSerialiserFromKey(final byte key) throws GafferCheckedException {
        Class<? extends ToBytesSerialiser> serialiserClass = keyMap.get(key);
        try {
            return (null == serialiserClass) ? null : serialiserClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GafferCheckedException(String.format("Unable to Instantiate serialiser, this means that serialiser was not checked within the put method: %s", serialiserClass));
        }
    }

    public ToBytesSerialiser getSerialiserFromValue(final Object object) throws GafferCheckedException {
        if (null == object) {
            return null;
        }
        Class<?> objectClass = object.getClass();
        Class<? extends ToBytesSerialiser> serialiserClass = classMap.get(objectClass);
        try {
            return (null == serialiserClass) ? null : serialiserClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GafferCheckedException(String.format("Unable to Instantiate serialiser, this means that serialiser was not checked within the put method: %s", serialiserClass));
        }
    }

    public boolean canHandle(final Class handleClass) throws GafferCheckedException {
        boolean rtn = false;
        for (Class<? extends ToBytesSerialiser> serialiserClass : keyMap.values()) {
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

    public byte getKeyFromSerialiser(final Object object) {
        Class<?> aClass = object.getClass();
        for (Entry<Byte, Class<? extends ToBytesSerialiser>> entry : keyMap.entrySet()) {
            if (entry.getValue().equals(aClass)) {
                return entry.getKey();
            }
        }
        throw null;
    }

    public boolean continuesToPreserveOrdering(final ToBytesSerialiser toBytesSerialiser) {
        return preservesObjectOrdering && toBytesSerialiser.preservesObjectOrdering();
    }

    public boolean continuesToBeConsistant(final ToBytesSerialiser toBytesSerialiser) {
        return consistent && toBytesSerialiser.isConsistent();
    }

    public boolean isPreservesObjectOrdering() {
        return preservesObjectOrdering;
    }

    public boolean isConsistent() {
        return consistent;
    }
}
