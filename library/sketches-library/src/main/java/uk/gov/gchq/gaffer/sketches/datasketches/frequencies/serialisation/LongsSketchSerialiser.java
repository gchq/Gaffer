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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.serialisation;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.frequencies.LongsSketch;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

/**
 * A <code>LongsSketchSerialiser</code> serialises a {@link LongsSketch} using its <code>toByteArray()</code>
 * method.
 */
public class LongsSketchSerialiser implements Serialisation<LongsSketch> {
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
        return LongsSketch.getInstance(new NativeMemory(bytes));
    }

    @Override
    public LongsSketch deserialiseEmptyBytes() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
