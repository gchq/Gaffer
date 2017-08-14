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

package uk.gov.gchq.gaffer.parquetstore.serialisation.impl;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser;
import java.io.IOException;

/**
 * This class is used to serialise and de-serialise a {@link HyperLogLogPlus} value for use by the
 * {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}, storing the sketch as a {@link byte[]} and the cardinality as a
 * {@link long} as a nested group containing two columns.
 */
public class NestedHyperLogLogPlusParquetSerialiser implements ParquetSerialiser<HyperLogLogPlus> {
    private static final long serialVersionUID = -8284005451029455563L;

    // This is not the best way to represent HLLP, this just allows for the testing of nested properties
    @Override
    public String getParquetSchema(final String colName) {
        return "optional group " + colName + " {\n" +
                "\toptional binary raw_bytes;\n" +
                "\toptional int64 cardinality;\n" +
                "}";
    }

    @Override
    public Object[] serialise(final HyperLogLogPlus object) throws SerialisationException {
        try {
            if (object != null) {
                return new Object[]{object.getBytes(), object.cardinality()};
            }
        } catch (final IOException e) {
            throw new SerialisationException("Failed to get bytes from the HyperLogLogPlus object.");
        }
        return new Object[]{null};
    }

    @Override
    public HyperLogLogPlus deserialise(final Object[] objects) throws SerialisationException {
        try {
            if (objects.length == 2 && objects[0] instanceof byte[]) {
                return HyperLogLogPlus.Builder.build(((byte[]) objects[0]));
            } else if (objects.length == 2 && objects[0] == null) {
                return null;
            }
            throw new SerialisationException("Could not de-serialise the HyperLogLogPlus object from objects");
        } catch (final SerialisationException e) {
            throw e;
        } catch (final IOException e) {
            throw new SerialisationException("Could not de-serialise the HyperLogLogPlus object from byte[]");
        }
    }

    @Override
    public HyperLogLogPlus deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a HyperLogLogPlus");
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public Object[] serialiseNull() {
        return new Object[0];
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return HyperLogLogPlus.class.equals(clazz);
    }
}
