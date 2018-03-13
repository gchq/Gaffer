/*
 * Copyright 2017-2018 Crown Copyright
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

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.sampling.ReservoirItemsUnion;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code ReservoirNumbersUnionSerialiser} serialises a {@link ReservoirItemsUnion} of {@link Number}s using its
 * {@code toByteArray()} method.
 */
public class ReservoirNumbersUnionSerialiser implements ToBytesSerialiser<ReservoirItemsUnion<Number>> {
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
        return ReservoirItemsUnion.heapify(WritableMemory.wrap(bytes), SERIALISER);
    }

    @Override
    public ReservoirItemsUnion<Number> deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }
}
