/*
 * Copyright 2016 Crown Copyright
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

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

/**
 * A <code>UnionSerialiser</code> serialises an {@link Union} using the <code>toByteArray()</code> method from the
 * sketch
 */
public class UnionSerialiser implements Serialisation<Union> {
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
        union.update(Sketch.heapify(new NativeMemory(bytes)));
        return union;
    }

    @Override
    public Union deserialiseEmptyBytes() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
