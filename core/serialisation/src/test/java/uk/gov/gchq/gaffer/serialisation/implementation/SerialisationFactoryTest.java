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
package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawIntegerSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SerialisationFactoryTest {
    @Test
    public void shouldReturnSerialiserForAString() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new SerialisationFactory();
        final Class<?> clazz = String.class;

        // When
        final Serialisation serialiser = factory.getSerialiser(clazz);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(StringSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldReturnOrderedSerialiserForAString() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new SerialisationFactory();
        final Class<?> clazz = String.class;
        final boolean ordered = true;

        // When
        final Serialisation serialiser = factory.getSerialiser(clazz, ordered);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(StringSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldReturnSerialiserForAnInteger() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new SerialisationFactory();
        final Class<?> clazz = Integer.class;

        // When
        final Serialisation serialiser = factory.getSerialiser(clazz);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(CompactRawIntegerSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldReturnOrderedSerialiserForAnInteger() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new SerialisationFactory();
        final Class<?> clazz = Integer.class;
        final boolean ordered = true;

        // When
        final Serialisation serialiser = factory.getSerialiser(clazz, ordered);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(RawIntegerSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldThrowExceptionIfClassIsNull() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new SerialisationFactory();
        final Class<?> clazz = null;

        // When / Then
        try {
            factory.getSerialiser(clazz);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfNoSerialiserFound() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new SerialisationFactory();
        final Class<?> clazz = Object.class;

        // When / Then
        try {
            factory.getSerialiser(clazz);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }
}