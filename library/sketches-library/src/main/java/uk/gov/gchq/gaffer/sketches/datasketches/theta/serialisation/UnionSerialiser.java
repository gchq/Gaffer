/*
 * Copyright 2016-2023 Crown Copyright
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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code UnionSerialiser} serialises an {@link Union} using the {@code toByteArray()} method from the
 * sketch
 */
public class UnionSerialiser implements ToBytesSerialiser<Union> {
    private static final long serialVersionUID = -7510002118163110532L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Union.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Union union) throws SerialisationException {
        return union.getResult().toByteArray();
    }

    @Override
    public Union deserialise(final byte[] bytes) throws SerialisationException {
        final Union union = Sketches.setOperationBuilder().buildUnion();
        union.union(WritableMemory.writableWrap(bytes));
        return union;
    }

    @Override
    public Union deserialiseEmpty() throws SerialisationException {
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
        return UnionSerialiser.class.getName().hashCode();
    }
}
