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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.frequencies.LongsSketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code LongsSketchSerialiser} serialises a {@link LongsSketch} using its {@code toByteArray()}
 * method.
 */
public class LongsSketchSerialiser implements ToBytesSerialiser<LongsSketch> {
    private static final long serialVersionUID = 4854333990248992270L;

    @Override
    public boolean canHandle(final Class clazz) {
        return LongsSketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final LongsSketch sketch) throws SerialisationException {
        return sketch.toByteArray();
    }

    @Override
    public LongsSketch deserialise(final byte[] bytes) throws SerialisationException {
        return LongsSketch.getInstance(WritableMemory.wrap(bytes));
    }

    @Override
    public LongsSketch deserialiseEmpty() throws SerialisationException {
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
        return LongsSketchSerialiser.class.getName().hashCode();
    }
}
