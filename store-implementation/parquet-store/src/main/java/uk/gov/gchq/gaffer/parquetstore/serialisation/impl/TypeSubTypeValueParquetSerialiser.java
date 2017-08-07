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
import java.util.ArrayList;

public class TypeSubTypeValueParquetSerialiser implements ParquetSerialiser<TypeSubTypeValue> {
    private static final long serialVersionUID = -3115394457831438674L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional TypeSubTypeValue " + colName + ";";
    }

    @Override
    public Object[] serialiseNull() {
        return new Object[0];
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return TypeSubTypeValue.class.equals(clazz);
    }

    @Override
    public Object[] serialise(final TypeSubTypeValue object) throws SerialisationException {
        return new Object[]{object};
    }

    @Override
    public TypeSubTypeValue deserialise(final Object[] objects) throws SerialisationException {
        if (objects.length == 1) {
            if (objects[0] instanceof ArrayList) {
                return (TypeSubTypeValue) objects[0];
            } else if (objects[0] == null) {
                return null;
            }
        }
        throw new SerialisationException("Could not de-serialise objects to a TypeSubTypeValue");
    }

    @Override
    public TypeSubTypeValue deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a TypeSubTypeValue");
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }
}
