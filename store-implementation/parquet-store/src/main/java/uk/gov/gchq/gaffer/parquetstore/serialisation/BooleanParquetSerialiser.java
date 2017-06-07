/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.serialisation;

import uk.gov.gchq.gaffer.exception.SerialisationException;

/**
 *
 */
public class BooleanParquetSerialiser implements ParquetSerialiser<Boolean> {

    private static final long serialVersionUID = -940386367544733514L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional boolean " + colName + ";";
    }

    @Override
    public Object[] serialise(final Boolean object) throws SerialisationException {
        final Object[] parquetObjects = new Object[1];
        parquetObjects[0] = object;
        return parquetObjects;
    }

    @Override
    public Boolean deserialise(final Object[] objects) throws SerialisationException {
        if (objects.length == 1 && objects[0] instanceof Boolean) {
            return (Boolean) objects[0];
        }
        throw new SerialisationException("Could not getPOJOFromParquetObjects");
    }

    @Override
    public Boolean deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Trying to de-serialise null to a boolean");
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
        return Boolean.class.equals(clazz);
    }
}
