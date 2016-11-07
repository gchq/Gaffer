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

package gaffer.store.operations;

import gaffer.commonutil.StreamUtil;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.store.operation.handler.generate.GenerateElementsHandler;
import gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import gaffer.store.operationdeclaration.OperationDeclaration;
import gaffer.store.operationdeclaration.OperationDeclarations;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationDeclarationJsonTest {
    private static final Logger LOG = Logger.getLogger(OperationDeclarationJsonTest.class);

    private final JSONSerialiser json = new JSONSerialiser();

    @Test
    public void testSerialiseDeserialise() throws SerialisationException {
        final OperationDeclaration declaration = new OperationDeclaration.Builder()
                .handler(new GenerateElementsHandler())
                .operation(GenerateElements.class)
                .build();

        byte[] definitionJson = json.serialise(declaration);

        LOG.info(new String(definitionJson));

        final OperationDeclaration deserialised = json.deserialise(definitionJson, OperationDeclaration.class);
        assertEquals(GenerateElements.class, deserialised.getOperation());
        assertTrue(deserialised.getHandler() instanceof GenerateElementsHandler);
    }

    @Test
    public void testDeserialiseFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "operationDeclarations.json");
        final OperationDeclarations deserialised = json.deserialise(s, OperationDeclarations.class);

        assertEquals(2, deserialised.getOperations().size());

        final OperationDeclaration od0 = deserialised.getOperations().get(0);
        final OperationDeclaration od1 = deserialised.getOperations().get(1);

        assertEquals(GenerateElements.class, od0.getOperation());
        assertTrue(od0.getHandler() instanceof GenerateElementsHandler);

        assertEquals(GenerateObjects.class, od1.getOperation());
        assertTrue(od1.getHandler() instanceof GenerateObjectsHandler);
    }
}
