/*
 * Copyright 2017-2023 Crown Copyright
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

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.ItemsSketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.util.Comparator;

/**
 * A {@code StringsSketchSerialiser} serialises an {@link org.apache.datasketches.quantiles.ItemsSketch} of
 * {@link String}s using its {@code toByteArray()} method.
 */
public class StringsSketchSerialiser implements ToBytesSerialiser<ItemsSketch<String>> {
    private static final long serialVersionUID = 7091724743812159058L;
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
       return ItemsSketch.getInstance(String.class, WritableMemory.writableWrap(bytes), Comparator.naturalOrder(), SERIALISER);
    }

    @Override
    public ItemsSketch<String> deserialiseEmpty() throws SerialisationException {
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
        return StringsSketchSerialiser.class.getName().hashCode();
    }
}
