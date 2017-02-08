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
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.frequencies.ItemsSketch;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

/**
 * A <code>StringsSketchSerialiser</code> serialises a {@link ItemsSketch} of {@link String}s using its
 * <code>toByteArray()</code> method.
 */
public class StringsSketchSerialiser implements Serialisation<ItemsSketch<String>> {
    private static final long serialVersionUID = 5678687223070030982L;
    private static final ArrayOfStringsSerDe SERIALISER = new ArrayOfStringsSerDe();

    @Override
    public boolean canHandle(final Class clazz) {
        return ItemsSketch.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final ItemsSketch<String> sketch) throws SerialisationException {
        return sketch.toByteArray(SERIALISER);
    }

    @Override
    public ItemsSketch<String> deserialise(final byte[] bytes) throws SerialisationException {
        return ItemsSketch.getInstance(new NativeMemory(bytes), SERIALISER);
    }

    @Override
    public ItemsSketch<String> deserialiseEmptyBytes() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
