/*
 * Copyright 2018 Crown Copyright
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
import com.yahoo.sketches.kll.KllFloatsSketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code KllFloatsSketchSerialiser} serialises a {@link KllFloatsSketch} using its {@code toByteArray()}
 * method.
 */
public class KllFloatsSketchSerialiser implements ToBytesSerialiser<KllFloatsSketch> {
    private static final long serialVersionUID = -5676465083474887650L;

    @Override
    public boolean canHandle(final Class clazz) {
        return KllFloatsSketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final KllFloatsSketch sketch) throws SerialisationException {
        return sketch.toByteArray();
    }

    @Override
    public KllFloatsSketch deserialise(final byte[] bytes) throws SerialisationException {
        return KllFloatsSketch.heapify(Memory.wrap(bytes));
    }

    @Override
    public KllFloatsSketch deserialiseEmpty() throws SerialisationException {
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

