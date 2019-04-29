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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code DoublesSketchSerialiser} serialises a {@link DoublesSketch} using its {@code toByteArray()}
 * method.
 */
public class DoublesSketchSerialiser implements ToBytesSerialiser<DoublesSketch> {
    private static final long serialVersionUID = -8961920881003350151L;

    @Override
    public boolean canHandle(final Class clazz) {
        return DoublesSketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final DoublesSketch sketch) throws SerialisationException {
        return sketch.toByteArray();
    }

    @Override
    public DoublesSketch deserialise(final byte[] bytes) throws SerialisationException {
        return DoublesSketch.wrap(Memory.wrap(bytes));
    }

    @Override
    public DoublesSketch deserialiseEmpty() throws SerialisationException {
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
        return DoublesSketchSerialiser.class.getName().hashCode();
    }
}

