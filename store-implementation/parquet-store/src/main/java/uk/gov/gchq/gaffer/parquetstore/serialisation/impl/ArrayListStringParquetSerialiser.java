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

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser;
import java.util.ArrayList;

/**
 * This class is used to serialise and de-serialise a {@link ArrayList} value for use by the
 * {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}.
 */
public class ArrayListStringParquetSerialiser implements ParquetSerialiser<ArrayList<String>> {
    private static final long serialVersionUID = -1415767993602827390L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional group " + colName + " (LIST) {\n" +
                " repeated group list {\n" +
                "  optional binary element (UTF8);\n" +
                " }\n" +
                "}";
    }

    @Override
    public Object[] serialise(final ArrayList<String> object) throws SerialisationException {
        if (object != null) {
            final String[] objects = new String[object.size()];
            object.toArray(objects);
            return new Object[]{objects};
        }
        return new Object[]{null};
    }

    @Override
    public ArrayList<String> deserialise(final Object[] objects) throws SerialisationException {

        if (objects.length == 1) {
            if (objects[0] instanceof String[]) {
                final String[] objectsToDeserialise = (String[]) objects[0];
                return Lists.newArrayList(objectsToDeserialise);
            } else if (objects[0] == null) {
                return null;
            }
        }
        throw new SerialisationException("Could not de-serialise objects to an ArrayList<String");
    }

    @Override
    public ArrayList<String> deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to an ArrayList<String>");
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
        return ArrayList.class.equals(clazz);
    }
}
