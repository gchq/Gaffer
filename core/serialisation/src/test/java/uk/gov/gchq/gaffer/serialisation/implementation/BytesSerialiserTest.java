/*
 * Copyright 2016-2018 Crown Copyright
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class BytesSerialiserTest extends ToBytesSerialisationTest<byte[]> {

    @Test
    public void cantSerialiseLongClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(Long.class));
    }

    @Test
    public void canSerialiseByteArrayClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(byte[].class));
    }

    @Test
    public void shouldSerialiseBytesByJustReturningTheProvidedBytes() throws SerialisationException {
        // Given
        final byte[] bytes = {0, 1};

        // When
        final byte[] serialisedBytes = serialiser.serialise(bytes);

        // Then
        assertSame(bytes, serialisedBytes);
    }

    @Test
    public void shouldDeerialiseBytesByJustReturningTheProvidedBytes() throws SerialisationException {
        // Given
        final byte[] bytes = {0, 1};

        // When
        final byte[] deserialisedBytes = serialiser.deserialise(bytes);

        // Then
        assertSame(bytes, deserialisedBytes);
    }

    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        // When
        final byte[] value = serialiser.deserialiseEmpty();

        // Then
        assertEquals(0, value.length);
    }

    @Override
    public Serialiser<byte[], byte[]> getSerialisation() {
        return new BytesSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<byte[], byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair(new byte[]{1, 2, 3, 4, 5, 6}, new byte[]{1, 2, 3, 4, 5, 6}),
                new Pair(new byte[]{12, 31, 43}, new byte[]{12, 31, 43}),
                new Pair(new byte[]{}, new byte[]{}),
                new Pair(new byte[]{122, -111, -33}, new byte[]{122, -111, -33})
        };
    }

    @Override
    protected void deserialiseSecond(final Pair<byte[], byte[]> pair) throws SerialisationException {
        assertArrayEquals(pair.getFirst(), serialiser.deserialise(pair.getSecond()));
    }
}
