/*
 * Copyright 2018-2019 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Delegate for the storage of {@link uk.gov.gchq.gaffer.serialisation.implementation.MultiSerialiser}
 */
public class MultiSerialiserStorage {
    public static final String ERROR_ADDING_MULTI_SERIALISER = "Adding nested MultiSerialiser within a MultiSerialiser is not supported yet.";
    private final Map<Byte, ToBytesSerialiser> keyToSerialiser = new HashMap<>();
    private final Map<Byte, Class> keyToClass = new HashMap<>();
    private final Map<Class, Byte> classToKey = new HashMap<>();
    private boolean consistent = true;
    private boolean preservesObjectOrdering = true;

    public void put(final byte serialiserEncoding, final ToBytesSerialiser serialiser, final Class supportedClass) throws GafferCheckedException {
        validatePutParams(serialiser, supportedClass);

        consistent = continuesToBeConsistant(serialiser);
        preservesObjectOrdering = continuesToPreserveOrdering(serialiser);

        keyToSerialiser.put(serialiserEncoding, serialiser);
        keyToClass.put(serialiserEncoding, supportedClass);
        classToKey.put(supportedClass, serialiserEncoding);
    }

    private void validatePutParams(final ToBytesSerialiser serialiser, final Class supportedClass) throws GafferCheckedException {
        if (null == supportedClass) {
            throw new GafferCheckedException(String.format("Can not add null supportedClass to MultiSerialiserStorage"));
        }
        if (null == serialiser) {
            throw new GafferCheckedException(String.format("Can not add null serialiser to MultiSerialiserStorage"));
        }
        if (!serialiser.canHandle(supportedClass)) {
            throw new GafferCheckedException(String.format("%s does not handle %s", serialiser.getClass(), supportedClass));
        }
    }

    public ToBytesSerialiser getSerialiserFromKey(final Byte key) {
        return (null == key) ? null : keyToSerialiser.get(key);
    }

    public Byte getKeyFromValue(final Object object) {
        return (null == object) ? null : classToKey.get(object.getClass());
    }

    public ToBytesSerialiser getSerialiserFromValue(final Object object) {
        return (null == object) ? null : getSerialiserFromKey(getKeyFromValue(object));
    }

    /**
     * @param handleClass class to check
     * @return {@link ToBytesSerialiser#canHandle(Class)}
     * @see ToBytesSerialiser
     */
    public boolean canHandle(final Class handleClass) {
        boolean rtn = false;
        final Byte key = classToKey.get(handleClass);
        if (null != key) {
            Serialiser serialiser = keyToSerialiser.get(key);
            rtn = null != serialiser && serialiser.canHandle(handleClass);
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

    public void addSerialiserDetails(final byte key, final ToBytesSerialiser serialiser, final Class aClass) throws GafferCheckedException {
        addSerialiserDetails(new SerialiserDetail(key, serialiser, aClass));
    }

    public void addSerialiserDetails(final SerialiserDetail serialiserDetail) throws GafferCheckedException {
        if (null != serialiserDetail) {
            if (serialiserDetail.getSerialiser() instanceof MultiSerialiser) {
                throw new GafferCheckedException(ERROR_ADDING_MULTI_SERIALISER);
            }
            put(serialiserDetail.getKey(), serialiserDetail.getSerialiser(), serialiserDetail.getValueClass());
        }
    }

    public List<SerialiserDetail> getSerialiserDetails() {
        return keyToSerialiser.keySet().stream()
                .map(key -> new SerialiserDetail(key, keyToSerialiser.get(key), keyToClass.get(key)))
                .collect(Collectors.toList());
    }

    public void setSerialiserDetails(final List<SerialiserDetail> serialisersDetails) throws GafferCheckedException {
        clear();
        if (null != serialisersDetails) {
            for (final SerialiserDetail serialiser : serialisersDetails) {
                addSerialiserDetails(serialiser);
            }
        }
    }

    private void clear() {
        keyToSerialiser.clear();
        keyToClass.clear();
        classToKey.clear();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final MultiSerialiserStorage serialiser = (MultiSerialiserStorage) obj;

        return new EqualsBuilder()
                .append(keyToClass, serialiser.keyToClass)
                .append(keyToSerialiser, serialiser.keyToSerialiser)
                .append(classToKey, serialiser.classToKey)
                .append(consistent, serialiser.consistent)
                .append(preservesObjectOrdering, serialiser.preservesObjectOrdering)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(keyToClass)
                .append(keyToSerialiser)
                .append(classToKey)
                .append(consistent)
                .append(preservesObjectOrdering)
                .toHashCode();
    }

    @JsonPropertyOrder(value = {"class", "key", "serialiser", "valueClass"}, alphabetic = true)
    public static class SerialiserDetail {
        private byte key;
        private ToBytesSerialiser serialiser;
        private Class valueClass;

        public SerialiserDetail() {
        }

        public SerialiserDetail(final byte key, final ToBytesSerialiser serialiser, final Class valueClass) {
            this();
            this.key = key;
            this.serialiser = serialiser;
            this.valueClass = valueClass;
        }

        public byte getKey() {
            return key;
        }

        public SerialiserDetail key(final byte key) {
            this.key = key;
            return this;
        }

        public Class getValueClass() {
            return valueClass;
        }

        public SerialiserDetail valueClass(final Class valueClass) {
            this.valueClass = valueClass;
            return this;
        }

        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public ToBytesSerialiser getSerialiser() {
            return serialiser;
        }

        public SerialiserDetail serialiser(final ToBytesSerialiser serialiser) {
            this.serialiser = serialiser;
            return this;
        }
    }
}
