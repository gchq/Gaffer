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

/**
 * This class is used to serialise and de-serialise a {@link Byte} value for use by the
 * {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}.
 */
public class ByteParquetSerialiser implements ParquetSerialiser<Byte> {
    private static final long serialVersionUID = -3905036281210562157L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional binary " + colName + ";";
    }

    @Override
    public Object[] serialise(final Byte object) throws SerialisationException {
        if (null != object) {
            final byte[] bytes = new byte[]{object};
            return new Object[]{bytes};
        } else {
            return new Object[]{null};
        }
    }

    @Override
    public Byte deserialise(final Object[] objects) throws SerialisationException {
        if (objects.length == 1) {
            if (objects[0] instanceof byte[] && ((byte[]) objects[0]).length <= 1) {
                return ((byte[]) objects[0])[0];
            } else if (null == objects[0]) {
                return null;
            }
        }
        throw new SerialisationException("Could not de-serialise objects to Byte");
    }

    @Override
    public Byte deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a Byte");
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
        return Byte.class.equals(clazz);
    }

    @Override
    public boolean isConsistent() {
        return true;
    }
}
