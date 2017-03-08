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

package uk.gov.gchq.gaffer.store.operations;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationDeclarationsTest {
    private final JSONSerialiser json = new JSONSerialiser();

    @Test
    public void testSerialiseDeserialise() throws SerialisationException {
        // Given
        final OperationDeclarations declarations = new OperationDeclarations.Builder()
                .declaration(new OperationDeclaration.Builder()
                        .handler(new GenerateElementsHandler())
                        .operation(GenerateElements.class)
                        .build())
                .build();

        // When
        final byte[] definitionJson = json.serialise(declarations);

        final OperationDeclarations deserialised = OperationDeclarations.fromJson(definitionJson);
        assertEquals(1, deserialised.getOperations().size());
        final OperationDeclaration deserialisedDeclaration = deserialised.getOperations().get(0);
        assertEquals(GenerateElements.class, deserialisedDeclaration.getOperation());
        assertTrue(deserialisedDeclaration.getHandler() instanceof GenerateElementsHandler);
    }

    @Test
    public void testDeserialiseFile() throws SerialisationException {
        // Given
        final String paths = "operationDeclarations1.json,operationDeclarations2.json";

        // When
        final OperationDeclarations deserialised = OperationDeclarations.fromPaths(paths);

        // Then
        assertEquals(2, deserialised.getOperations().size());

        final OperationDeclaration od0 = deserialised.getOperations().get(0);
        final OperationDeclaration od1 = deserialised.getOperations().get(1);

        assertEquals(GenerateElements.class, od0.getOperation());
        assertTrue(od0.getHandler() instanceof GenerateElementsHandler);

        assertEquals(GenerateObjects.class, od1.getOperation());
        assertTrue(od1.getHandler() instanceof GenerateObjectsHandler);
    }
}
