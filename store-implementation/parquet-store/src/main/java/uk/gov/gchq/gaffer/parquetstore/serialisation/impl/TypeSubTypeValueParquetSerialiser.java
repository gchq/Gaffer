/*
 * Copyright 2017 Crown Copyright
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
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

/**
 * This class is used to serialise and de-serialise a {@link TypeSubTypeValue} value for use by the
 * {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}.
 */
public class TypeSubTypeValueParquetSerialiser implements ParquetSerialiser<TypeSubTypeValue> {
    private static final long serialVersionUID = -3115394457831438674L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional binary " + colName + "_type (UTF8);\n" +
                "optional binary " + colName + "_subType (UTF8);\n" +
                "optional binary " + colName + "_value (UTF8);";
    }

    @Override
    public Object[] serialise(final TypeSubTypeValue object) throws SerialisationException {
        if (object != null) {
            return new Object[]{object.getType(), object.getSubType(), object.getValue()};
        }
        return new Object[]{null, null, null};
    }

    @Override
    public TypeSubTypeValue deserialise(final Object[] objects) throws SerialisationException {
        if (objects[0] == null) {
            return null;
        } else if (objects.length == 3) {
            return new TypeSubTypeValue(objects[0].toString(), objects[1].toString(), objects[2].toString());
        } else {
            throw new SerialisationException("Could not de-serialise objects to a TypeSubTypeValue");
        }

    }

    @Override
    public TypeSubTypeValue deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a TypeSubTypeValue");
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public Object[] serialiseNull() {
        return new Object[0];
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return TypeSubTypeValue.class.equals(clazz);
    }
}
