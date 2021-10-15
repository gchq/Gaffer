/*
 * Copyright 2017-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.serialisation.impl;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser;
import uk.gov.gchq.gaffer.types.TypeValue;

/**
 * This class is used to serialise and de-serialise a {@link TypeValue} value for use by the
 * {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}.
 */
public class TypeValueParquetSerialiser implements ParquetSerialiser<TypeValue> {
    private static final long serialVersionUID = 237193138266936512L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional binary " + colName + "_type (UTF8);\n" +
                "optional binary " + colName + "_value (UTF8);";
    }

    @Override
    public Object[] serialise(final TypeValue object) throws SerialisationException {
        if (null != object) {
            return new Object[]{object.getType(), object.getValue()};
        }
        return new Object[]{null, null};
    }

    @Override
    public TypeValue deserialise(final Object[] objects) throws SerialisationException {
        if (objects.length == 2) {
            if (objects[0] instanceof String && objects[1] instanceof String) {
                return new TypeValue((String) objects[0], (String) objects[1]);
            } else if (null == objects[0]) {
                return null;
            }
        }
        throw new SerialisationException("Could not de-serialise objects to a TypeValue");
    }

    @Override
    public TypeValue deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a TypeValue");
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }

    @Override
    public Object[] serialiseNull() {
        return new Object[0];
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return TypeValue.class.equals(clazz);
    }
}
