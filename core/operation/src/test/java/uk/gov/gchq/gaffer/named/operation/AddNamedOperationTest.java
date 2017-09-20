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

package uk.gov.gchq.gaffer.named.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class AddNamedOperationTest extends OperationTest<AddNamedOperation> {
    public static final String USER = "User";
    private static final OperationChain OPERATION_CHAIN = new OperationChain.Builder().first(new GetAdjacentIds.Builder().input(new EntitySeed("seed")).build()).build();

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        final AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .description("Test Named Operation")
                .name("Test")
                .overwrite()
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .build();

        // When
        String json = new String(JSONSerialiser.serialise(addNamedOperation, true));

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.named.operation.AddNamedOperation\",%n" +
                "  \"operationName\": \"Test\",%n" +
                "  \"description\": \"Test Named Operation\",%n" +
                "  \"readAccessRoles\": [\"User\"],%n" +
                "  \"writeAccessRoles\": [\"User\"],%n" +
                "  \"overwriteFlag\": true,%n" +
                "  \"operationChain\": {" +
                "  \"operations\": [{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\", \"input\": [{\"vertex\": \"seed\", \"class\": \"uk.gov.gchq.gaffer.operation.data.EntitySeed\"}]}]}" +
                "}"), json);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .description("Test Named Operation")
                .name("Test")
                .overwrite()
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .build();
        String opChain = null;
        try {
            opChain = new String(JSONSerialiser.serialise(new OperationChainDAO<>(OPERATION_CHAIN.getOperations())));
        } catch (final SerialisationException e) {
            fail();
        }
        assertEquals(opChain, addNamedOperation.getOperationChainAsString());
        assertEquals("Test", addNamedOperation.getOperationName());
        assertEquals("Test Named Operation", addNamedOperation.getDescription());
        assertEquals(Collections.singletonList(USER), addNamedOperation.getReadAccessRoles());
        assertEquals(Collections.singletonList(USER), addNamedOperation.getWriteAccessRoles());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        Map<String, ParameterDetail> parameters = new HashMap<>();
        parameters.put("testParameter", mock(ParameterDetail.class));

        AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .description("Test Named Operation")
                .name("Test")
                .overwrite(false)
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .parameters(parameters)
                .build();
        String opChain = null;
        try {
            opChain = new String(JSONSerialiser.serialise(new OperationChainDAO<>(OPERATION_CHAIN.getOperations())));
        } catch (final SerialisationException e) {
            fail();
        }

        // When
        AddNamedOperation clone = addNamedOperation.shallowClone();

        // Then
        assertNotSame(addNamedOperation, clone);
        assertEquals(opChain, clone.getOperationChainAsString());
        assertEquals("Test", clone.getOperationName());
        assertEquals("Test Named Operation", clone.getDescription());
        assertFalse(clone.isOverwriteFlag());
        assertEquals(Collections.singletonList(USER), clone.getReadAccessRoles());
        assertEquals(Collections.singletonList(USER), clone.getWriteAccessRoles());
        assertEquals(parameters, clone.getParameters());
    }

    @Override
    protected AddNamedOperation getTestObject() {
        return new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .build();
    }
}
