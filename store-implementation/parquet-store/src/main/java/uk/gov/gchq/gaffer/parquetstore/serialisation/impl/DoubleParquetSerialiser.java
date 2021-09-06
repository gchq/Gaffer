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
 * This class is used to serialise and de-serialise a {@link Double} value for use by the
 * {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}.
 */
public class DoubleParquetSerialiser implements ParquetSerialiser<Double> {
    private static final long serialVersionUID = 1832911259645511610L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional double " + colName + ";";
    }

    @Override
    public Object[] serialise(final Double object) throws SerialisationException {
        return new Object[]{object};
    }

    @Override
    public Double deserialise(final Object[] objects) throws SerialisationException {
        if (objects.length == 1) {
            if (objects[0] instanceof Double) {
                return (Double) objects[0];
            } else if (null == objects[0]) {
                return null;
            }
        }
        throw new SerialisationException("Could not de-serialise objects to a Double");
    }

    @Override
    public Double deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a Double");
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
        return Double.class.equals(clazz);
    }
}
