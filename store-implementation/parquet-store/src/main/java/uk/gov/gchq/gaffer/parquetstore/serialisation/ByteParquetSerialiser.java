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
public class ByteParquetSerialiser implements ParquetSerialiser<Byte> {

    private static final long serialVersionUID = -3905036281210562157L;

    @Override
    public Object[] serialiseNull() {
        return new Object[0];
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Byte.class.equals(clazz);
    }

    @Override
    public String getParquetSchema(final String colName) {
        return "optional binary " + colName + ";";
    }

    @Override
    public Object[] serialise(final Byte object) throws SerialisationException {
        final Object[] parquetObjects = new Object[1];
        if (object != null) {
            final byte[] bytes = new byte[1];
            bytes[0] = object;
            parquetObjects[0] = bytes;
        } else {
            parquetObjects[0] = null;
        }
        return parquetObjects;
    }

    @Override
    public Byte deserialise(final Object[] objects) throws SerialisationException {
        if (objects.length == 1) {
            if (objects[0] == null) {
                return null;
            } else if (objects[0] instanceof byte[] && ((byte[]) objects[0]).length <= 1) {
                return ((byte[]) objects[0])[0];
            } else {
                throw new SerialisationException("Too many bytes found only expected a single byte");
            }
        } else {
            throw new SerialisationException("Too many Objects found to de-serialise");
        }
    }

    @Override
    public Byte deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }
}
