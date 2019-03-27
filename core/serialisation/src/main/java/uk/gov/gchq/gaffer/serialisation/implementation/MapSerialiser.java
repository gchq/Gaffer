/*
 * Copyright 2016-2019 Crown Copyright
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
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class is used to serialise and deserialise {@link Map}s.
 */
public class MapSerialiser implements ToBytesSerialiser<Map> {

    private static final long serialVersionUID = 323888878024609587L;
    private ToBytesSerialiser keySerialiser;
    private ToBytesSerialiser valueSerialiser;
    private Class<? extends Map> mapClass;

    @Override
    public boolean canHandle(final Class clazz) {
        return Map.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final Map object) throws SerialisationException {
        LengthValueBytesSerialiserUtil.LengthValueBuilder builder = new LengthValueBytesSerialiserUtil.LengthValueBuilder();
        try {
            for (final Object o : object.entrySet()) {
                if (o instanceof Map.Entry) {
                    Map.Entry entry = (Map.Entry) o;
                    final ToBytesSerialiser keySerialiser = getKeySerialiser();
                    final ToBytesSerialiser valueSerialiser = getValueSerialiser();
                    checkSerialiers(keySerialiser, valueSerialiser);
                    builder.appendLengthValueFromObjectToByteStream(keySerialiser, entry.getKey());
                    builder.appendLengthValueFromObjectToByteStream(valueSerialiser, entry.getValue());
                } else {
                    throw new SerialisationException("Was not able to process EntrySet of Map");
                }
            }
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return builder.toArray();
    }

    protected void checkSerialiers(final ToBytesSerialiser keySerialiser, final ToBytesSerialiser valueSerialiser) {
        requireNonNull(keySerialiser, "keySerialiser has to been set.");
        requireNonNull(valueSerialiser, "valueSerialiser has to been set.");
    }

    @Override
    public Map<? extends Object, ? extends Object> deserialise(final byte[] bytes) throws SerialisationException {
        Map map;
        if (null == getMapClass()) {
            map = new HashMap<>();
        } else {
            try {
                map = getMapClass().newInstance();
            } catch (final IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                throw new SerialisationException("Failed to create map instance" + e.getMessage(), e);
            }
        }
        final int arrayLength = bytes.length;
        int carriage = 0;
        while (carriage < arrayLength) {
            final ToBytesSerialiser keySerialiser = getKeySerialiser();
            final ToBytesSerialiser valueSerialiser = getValueSerialiser();
            checkSerialiers(keySerialiser, valueSerialiser);
            LengthValueBytesSerialiserUtil.ObjectCarriage c = LengthValueBytesSerialiserUtil.deserialiseNextObject(keySerialiser, carriage, bytes);
            LengthValueBytesSerialiserUtil.ObjectCarriage c2 = LengthValueBytesSerialiserUtil.deserialiseNextObject(valueSerialiser, c.getCarriage(), bytes);
            map.put(c.getObject(), c2.getObject());
            carriage = c2.getCarriage();
        }
        return map;
    }


    @Override
    public Map<? extends Object, ? extends Object> deserialiseEmpty() throws SerialisationException {
        return null;
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
    public ToBytesSerialiser getKeySerialiser() {
        return keySerialiser;
    }

    @JsonIgnore
    public void setKeySerialiser(final ToBytesSerialiser keySerialiser) {
        this.keySerialiser = keySerialiser;
    }

    @JsonGetter("keySerialiser")
    public String getKeySerialiserClassString() {
        return null != keySerialiser ? SimpleClassNameIdResolver.getSimpleClassName(keySerialiser.getClass()) : null;
    }

    @JsonSetter("keySerialiser")
    public void setKeySerialiserClassString(final String classType) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.keySerialiser = null != classType ? Class.forName(SimpleClassNameIdResolver.getClassName(classType)).asSubclass(ToBytesSerialiser.class).newInstance() : null;
    }

    @JsonIgnore
    public ToBytesSerialiser getValueSerialiser() {
        return valueSerialiser;
    }

    @JsonIgnore
    public void setValueSerialiser(final ToBytesSerialiser valueSerialiser) {
        this.valueSerialiser = valueSerialiser;
    }

    @JsonGetter("valueSerialiser")
    public String getValueSerialiserClassString() {
        return null != valueSerialiser ? SimpleClassNameIdResolver.getSimpleClassName(valueSerialiser.getClass()) : null;
    }

    @JsonSetter("valueSerialiser")
    public void setValueSerialiserClassString(final String classType) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.valueSerialiser = null != classType ? Class.forName(SimpleClassNameIdResolver.getClassName(classType)).asSubclass(ToBytesSerialiser.class).newInstance() : null;
    }

    @JsonGetter("mapClass")
    public String getMapClassString() {
        return null != mapClass ? SimpleClassNameIdResolver.getSimpleClassName(mapClass) : null;
    }

    @JsonSetter("mapClass")
    public void setMapClassString(final String classType) throws ClassNotFoundException {
        this.mapClass = null != classType ? Class.forName(SimpleClassNameIdResolver.getClassName(classType)).asSubclass(Map.class) : null;
    }

    @JsonIgnore
    public Class<? extends Map> getMapClass() {
        return mapClass;
    }

    public void setMapClass(final Class<? extends Map> mapClass) {
        this.mapClass = mapClass;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final MapSerialiser serialiser = (MapSerialiser) obj;

        return new EqualsBuilder()
                .append(keySerialiser, serialiser.keySerialiser)
                .append(valueSerialiser, serialiser.valueSerialiser)
                .append(mapClass, serialiser.mapClass)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(keySerialiser)
                .append(valueSerialiser)
                .append(mapClass)
                .toHashCode();
    }
}
