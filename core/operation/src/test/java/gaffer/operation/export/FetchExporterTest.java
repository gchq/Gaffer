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

package gaffer.operation.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import gaffer.operation.impl.export.FetchExporter;
import org.junit.Test;


public class FetchExporterTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final FetchExporter op = new FetchExporter();

        // When
        byte[] json = serialiser.serialise(op, true);
        final FetchExporter deserialisedOp = serialiser.deserialise(json, FetchExporter.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        FetchExporter fetchExport = new FetchExporter.Builder().option("testOption", "true").build();
        assertEquals("true", fetchExport.getOption("testOption"));
    }
}
