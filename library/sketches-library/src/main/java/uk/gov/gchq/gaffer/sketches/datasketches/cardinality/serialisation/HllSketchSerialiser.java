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
package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation;

import com.yahoo.sketches.hll.HllSketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code HllSketchSerialiser} serialises a {@link com.yahoo.sketches.hll.HllSketch} using its
 * {@code toCompactByteArray()} method.
 */
public class HllSketchSerialiser implements ToBytesSerialiser<HllSketch> {
    private static final long serialVersionUID = 5903372368174309494L;

    @Override
    public boolean canHandle(final Class clazz) {
        return HllSketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final HllSketch sketch) throws SerialisationException {
        return sketch.toCompactByteArray();
    }

    @Override
    public HllSketch deserialise(final byte[] bytes) throws SerialisationException {
        return HllSketch.heapify(bytes);
    }

    @Override
    public HllSketch deserialiseEmpty() throws SerialisationException {
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
