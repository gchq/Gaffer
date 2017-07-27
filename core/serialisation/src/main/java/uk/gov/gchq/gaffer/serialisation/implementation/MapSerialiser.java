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
package uk.gov.gchq.gaffer.serialisation.implementation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapSerialiser implements ToBytesSerialiser<Map<? extends Object, ? extends Object>> {

    private static final long serialVersionUID = 323888878024609587L;
    private ToBytesSerialiser keySerialiser;
    private ToBytesSerialiser valueSerialiser;
    private Class<? extends Map> mapClass;

    @Override
    public boolean canHandle(final Class clazz) {
        return Map.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final Map<? extends Object, ? extends Object> object) throws SerialisationException {
        LengthValueBytesSerialiserUtil.LengthValueBuilder builder = new LengthValueBytesSerialiserUtil.LengthValueBuilder();
        try {
            for (final Map.Entry entry : object.entrySet()) {
                builder.appendLengthValueFromObjectToByteStream(getKeySerialiser(), entry.getKey());
                builder.appendLengthValueFromObjectToByteStream(getValueSerialiser(), entry.getValue());
            }
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return builder.toArray();
    }

    @Override
    public Map<? extends Object, ? extends Object> deserialise(final byte[] bytes) throws SerialisationException {
        Map map;
        if (getMapClass() == null) {
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
            LengthValueBytesSerialiserUtil.ObjectCarriage c = LengthValueBytesSerialiserUtil.deserialiseNextObject(getKeySerialiser(), carriage, bytes);
            LengthValueBytesSerialiserUtil.ObjectCarriage c2 = LengthValueBytesSerialiserUtil.deserialiseNextObject(getValueSerialiser(), c.getCarriage(), bytes);
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
        return null != keySerialiser ? keySerialiser.getClass().getName() : null;
    }

    @JsonSetter("keySerialiser")
    public void setKeySerialiserClassString(final String classType) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.keySerialiser = null != classType ? Class.forName(classType).asSubclass(ToBytesSerialiser.class).newInstance() : null;
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
        return null != valueSerialiser ? valueSerialiser.getClass().getName() : null;
    }

    @JsonSetter("valueSerialiser")
    public void setValueSerialiserClassString(final String classType) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.valueSerialiser = null != classType ? Class.forName(classType).asSubclass(ToBytesSerialiser.class).newInstance() : null;
    }

    @JsonGetter("mapClass")
    public String getMapClassString() {
        return null != mapClass ? mapClass.getName() : null;
    }

    @JsonSetter("mapClass")
    public void setMapClassString(final String classType) throws ClassNotFoundException {
        this.mapClass = null != classType ? Class.forName(classType).asSubclass(Map.class) : null;
    }

    @JsonIgnore
    public Class<? extends Map> getMapClass() {
        return mapClass;
    }

    public void setMapClass(final Class<? extends Map> mapClass) {
        this.mapClass = mapClass;
    }
}
