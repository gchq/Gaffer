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
import com.yahoo.sketches.sampling.ReservoirLongsSketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code ReservoirLongsSketchSerialiser} serialises a {@link ReservoirLongsSketch} using its
 * {@code toByteArray()} method.
 */
public class ReservoirLongsSketchSerialiser implements ToBytesSerialiser<ReservoirLongsSketch> {
    private static final long serialVersionUID = 2492278033004791488L;

    @Override
    public boolean canHandle(final Class clazz) {
        return ReservoirLongsSketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final ReservoirLongsSketch sketch) throws SerialisationException {
        return sketch.toByteArray();
    }

    @Override
    public ReservoirLongsSketch deserialise(final byte[] bytes) throws SerialisationException {
        return ReservoirLongsSketch.heapify(WritableMemory.wrap(bytes));
    }

    @Override
    public ReservoirLongsSketch deserialiseEmpty() throws SerialisationException {
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

