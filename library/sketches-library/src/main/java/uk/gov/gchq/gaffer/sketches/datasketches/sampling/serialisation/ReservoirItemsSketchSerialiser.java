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
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code ReservoirItemsSketchSerialiser} serialises a {@link ReservoirItemsSketch} using its
 * {@code toByteArray()} method and the provided {@link ArrayOfItemsSerDe}.
 */
public class ReservoirItemsSketchSerialiser<T> implements ToBytesSerialiser<ReservoirItemsSketch<T>> {
    private final ArrayOfItemsSerDe<T> arrayOfItemsSerDe;

    public ReservoirItemsSketchSerialiser(final ArrayOfItemsSerDe<T> arrayOfItemsSerDe) {
        this.arrayOfItemsSerDe = arrayOfItemsSerDe;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return ReservoirItemsSketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final ReservoirItemsSketch<T> sketch) throws SerialisationException {
        return sketch.toByteArray(arrayOfItemsSerDe);
    }

    @Override
    public ReservoirItemsSketch<T> deserialise(final byte[] bytes) throws SerialisationException {
        return ReservoirItemsSketch.heapify(WritableMemory.wrap(bytes), arrayOfItemsSerDe);
    }

    @Override
    public ReservoirItemsSketch<T> deserialiseEmpty() throws SerialisationException {
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
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final ReservoirItemsSketchSerialiser serialiser = (ReservoirItemsSketchSerialiser) obj;

        return new EqualsBuilder()
                .append(arrayOfItemsSerDe, serialiser.arrayOfItemsSerDe)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(arrayOfItemsSerDe)
                .toHashCode();
    }
}
