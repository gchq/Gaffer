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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SetSerialiser implements ToBytesSerialiser<Set<? extends Object>> {

    private static final long serialVersionUID = -8681798703430202402L;
    private ToBytesSerialiser objectSerialiser;
    private Class<? extends Set> setClass;

    public SetSerialiser() {
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Set.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final Set<? extends Object> object) throws SerialisationException {
        LengthValueBytesSerialiserUtil.LengthValueBuilder builder = new LengthValueBytesSerialiserUtil.LengthValueBuilder();
        try {
            for (final Object entry : object) {
                builder.appendLengthValueFromObjectToByteStream(getObjectSerialiser(), entry);
            }
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return builder.toArray();
    }

    @Override
    public Set<? extends Object> deserialise(final byte[] bytes) throws SerialisationException {
        Set set;
        if (null == getSetClass()) {
            set = new HashSet<>();
        } else {
            try {
                set = getSetClass().newInstance();
            } catch (final IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                throw new SerialisationException("Failed to create map instance" + e.getMessage(), e);
            }
        }
        final int arrayLength = bytes.length;
        int carriage = 0;
        while (carriage < arrayLength) {
            LengthValueBytesSerialiserUtil.ObjectCarriage c = LengthValueBytesSerialiserUtil.deserialiseNextObject(getObjectSerialiser(), carriage, bytes);
            set.add(c.getObject());
            carriage = c.getCarriage();
        }
        return set;
    }


    @Override
    public Set<? extends Object> deserialiseEmpty() throws SerialisationException {
        Set set;
        if (null == getSetClass()) {
            set = new HashSet<>();
        } else {
            try {
                set = getSetClass().newInstance();
            } catch (final IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                throw new SerialisationException("Failed to create map instance" + e.getMessage(), e);
            }
        }
        return set;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }

    @JsonIgnore
    public ToBytesSerialiser getObjectSerialiser() {
        return objectSerialiser;
    }

    @JsonIgnore
    public void setObjectSerialiser(final ToBytesSerialiser objectSerialiser) {
        this.objectSerialiser = objectSerialiser;
    }

    @JsonGetter("objectSerialiser")
    public String getObjectSerialiserClassString() {
        return null != objectSerialiser ? SimpleClassNameIdResolver.getSimpleClassName(objectSerialiser.getClass()) : null;
    }

    @JsonSetter("objectSerialiser")
    public void setObjectSerialiserClassString(final String classType) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.objectSerialiser = null != classType ? Class.forName(SimpleClassNameIdResolver.getClassName(classType)).asSubclass(ToBytesSerialiser.class).newInstance() : null;
    }


    @JsonGetter("setClass")
    public String getSetClassString() {
        return null != setClass ? SimpleClassNameIdResolver.getSimpleClassName(setClass) : null;
    }

    @JsonSetter("setClass")
    public void setSetClassString(final String classType) throws ClassNotFoundException {
        this.setClass = null != classType ? Class.forName(SimpleClassNameIdResolver.getClassName(classType)).asSubclass(Set.class) : null;
    }

    @JsonIgnore
    public Class<? extends Set> getSetClass() {
        return setClass;
    }

    @JsonIgnore
    public void setSetClass(final Class<? extends Set> setClass) {
        this.setClass = setClass;
    }
}
