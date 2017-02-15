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

package uk.gov.gchq.gaffer.operation.export;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.export.FetchExport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class FetchExportTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();
    private static final String INVALID_KEY = "/invalidKey";

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final String key = "key";
        final FetchExport op = new FetchExport(key);

        // When
        byte[] json = serialiser.serialise(op, true);
        final FetchExport deserialisedOp = serialiser.deserialise(json, FetchExport.class);

        // Then
        assertEquals(key, deserialisedOp.getKey());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        FetchExport operation = new FetchExport.Builder().key("Test").option("testOption", "true").build();
        assertEquals("Test", operation.getKey());
        assertEquals("true", operation.getOption("testOption"));
    }

    @Test
    public void shouldRejectInvalidKeyInConstructor() {
        // Given
        final String invalidKey = INVALID_KEY;

        try {
            new FetchExport(invalidKey);
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldRejectInvalidKeyInSetter() {
        // Given
        final String invalidKey = INVALID_KEY;

        try {
            new FetchExport().setKey(invalidKey);
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void builderShouldRejectInvalidKey() {
        // Given
        final String invalidKey = INVALID_KEY;

        // When / Then
        try {
            new FetchExport.Builder()
                    .key(invalidKey)
                    .build();
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }
}
