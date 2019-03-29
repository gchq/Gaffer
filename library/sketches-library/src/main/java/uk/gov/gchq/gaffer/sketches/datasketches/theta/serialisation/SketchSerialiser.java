/*
 * Copyright 2016-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.serialisation;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.theta.Sketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code SketchSerialiser} serialises a {@link com.yahoo.sketches.theta.Sketch} using its
 * {@code toByteArray()}.
 */
public class SketchSerialiser implements ToBytesSerialiser<Sketch> {
    private static final long serialVersionUID = 7334348024327614467L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Sketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Sketch sketch) throws SerialisationException {
        return sketch.toByteArray();
    }

    @Override
    public Sketch deserialise(final byte[] bytes) throws SerialisationException {
        return Sketch.wrap(Memory.wrap(bytes));
    }

    @Override
    public Sketch deserialiseEmpty() throws SerialisationException {
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
        return SketchSerialiser.class.getName().hashCode();
    }
}
