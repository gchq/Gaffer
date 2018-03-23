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
package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NullSerialiserTest extends ToBytesSerialisationTest<Object> {

    @Override
    public Serialiser<Object, byte[]> getSerialisation() {
        return new NullSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Object, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair("", new byte[]{}),
                new Pair(null, new byte[]{}),
                new Pair("some string", new byte[]{}),
                new Pair(1L, new byte[]{}),
                new Pair(0, new byte[]{}),
                new Pair(true, new byte[]{}),
                new Pair(false, new byte[]{})
        };
    }

    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        assertNull(serialiser.deserialiseEmpty());
    }

    @Test
    public void shouldHandleAnyClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(String.class));
        assertTrue(serialiser.canHandle(Object.class));
        assertTrue(serialiser.canHandle(Integer.class));
    }

    @Test
    public void shouldBeConsistent() throws SerialisationException {
        assertTrue(serialiser.isConsistent());
    }

    @Test
    public void shouldPreserveOrdering() throws SerialisationException {
        assertTrue(serialiser.preservesObjectOrdering());
    }

    @Override
    @Test
    public void shouldSerialiseWithHistoricValues() throws Exception {
        for (final Pair<Object, byte[]> pair : historicSerialisationPairs) {
            serialiseFirst(pair);
            assertNull(serialiser.deserialise(pair.getSecond()));
        }
    }
}
