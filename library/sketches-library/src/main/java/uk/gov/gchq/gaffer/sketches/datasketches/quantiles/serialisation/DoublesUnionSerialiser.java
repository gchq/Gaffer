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
import com.yahoo.sketches.quantiles.DoublesUnion;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code DoublesUnionSerialiser} serialises a {@link DoublesUnion} using its {@code toByteArray()}
 * method.
 */
public class DoublesUnionSerialiser implements ToBytesSerialiser<DoublesUnion> {
    private static final long serialVersionUID = 7855827433100904609L;

    @Override
    public boolean canHandle(final Class clazz) {
        return DoublesUnion.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final DoublesUnion union) throws SerialisationException {
        return union.getResult().toByteArray();
    }

    @Override
    public DoublesUnion deserialise(final byte[] bytes) throws SerialisationException {
        final DoublesUnion union = DoublesUnion.builder().build();
        union.update(WritableMemory.wrap(bytes));
        return union;
    }

    @Override
    public DoublesUnion deserialiseEmpty() throws SerialisationException {
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

