/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToByteSerialisationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ByteSerialiserTest extends ToByteSerialisationTest<byte[]> {

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
}