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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.sampling.ReservoirItemsUnion;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

/**
 * A <code>ReservoirNumbersUnionSerialiser</code> serialises a {@link ReservoirItemsUnion} of {@link Number}s using its
 * <code>toByteArray()</code> method.
 */
public class ReservoirNumbersUnionSerialiser implements Serialisation<ReservoirItemsUnion<Number>> {
    private static final long serialVersionUID = -1935225742362536044L;
    private static final ArrayOfNumbersSerDe SERIALISER = new ArrayOfNumbersSerDe();

    @Override
    public boolean canHandle(final Class clazz) {
        return ReservoirItemsUnion.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final ReservoirItemsUnion<Number> union) throws SerialisationException {
        return union.toByteArray(SERIALISER);
    }

    @Override
    public ReservoirItemsUnion<Number> deserialise(final byte[] bytes) throws SerialisationException {
        return ReservoirItemsUnion.getInstance(new NativeMemory(bytes), SERIALISER);
    }

    @Override
    public ReservoirItemsUnion<Number> deserialiseEmptyBytes() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
