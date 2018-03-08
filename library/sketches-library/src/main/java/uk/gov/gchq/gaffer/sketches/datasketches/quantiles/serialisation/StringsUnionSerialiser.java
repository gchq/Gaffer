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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.serialisation;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsUnion;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.util.Comparator;

/**
 * A {@code StringsUnionSerialiser} serialises an {@link ItemsUnion} of {@link String}s using its
 * {@code toByteArray()} method.
 */
public class StringsUnionSerialiser implements ToBytesSerialiser<ItemsUnion<String>> {
    private static final long serialVersionUID = 7091724743812159058L;
    private static final ArrayOfStringsSerDe SERIALISER = new ArrayOfStringsSerDe();

    @Override
    public boolean canHandle(final Class clazz) {
        return ItemsUnion.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final ItemsUnion<String> union) throws SerialisationException {
        return union.getResult().toByteArray(SERIALISER);
    }

    @Override
    public ItemsUnion<String> deserialise(final byte[] bytes) throws SerialisationException {
       return ItemsUnion.getInstance(WritableMemory.wrap(bytes), Comparator.naturalOrder(), SERIALISER);
    }

    @Override
    public ItemsUnion<String> deserialiseEmpty() throws SerialisationException {
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
