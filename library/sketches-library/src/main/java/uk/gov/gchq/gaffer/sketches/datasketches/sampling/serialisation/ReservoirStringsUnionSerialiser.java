/*
 * Copyright 2017-2019 Crown Copyright
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
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.sampling.ReservoirItemsUnion;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code ReservoirStringsUnionSerialiser} serialises a {@link ReservoirItemsUnion} of {@link String}s using its
 * {@code toByteArray()} method.
 */
public class ReservoirStringsUnionSerialiser implements ToBytesSerialiser<ReservoirItemsUnion<String>> {
    private static final long serialVersionUID = 5669266109027616942L;
    private static final ArrayOfStringsSerDe SERIALISER = new ArrayOfStringsSerDe();

    @Override
    public boolean canHandle(final Class clazz) {
        return ReservoirItemsUnion.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final ReservoirItemsUnion<String> union) throws SerialisationException {
        return union.toByteArray(SERIALISER);
    }

    @Override
    public ReservoirItemsUnion<String> deserialise(final byte[] bytes) throws SerialisationException {
        return ReservoirItemsUnion.heapify(WritableMemory.wrap(bytes), SERIALISER);
    }

    @Override
    public ReservoirItemsUnion<String> deserialiseEmpty() throws SerialisationException {
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

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return ReservoirStringsUnionSerialiser.class.getName().hashCode();
    }
}
