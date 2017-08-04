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
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AddNamedOperationTest extends OperationTest<AddNamedOperation> {

    private static final JSONSerialiser serialiser = new JSONSerialiser();
    private static final OperationChain OPERATION_CHAIN = new OperationChain.Builder().first(new GetAdjacentIds.Builder().input(new EntitySeed("seed")).build()).build();

    public static final String USER = "User";

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
        String json = new String(serialiser.serialise(addNamedOperation, true));

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.named.operation.AddNamedOperation\",%n" +
                "  \"operationName\": \"Test\",%n" +
                "  \"description\": \"Test Named Operation\",%n" +
                "  \"readAccessRoles\": [\"User\"],%n" +
                "  \"writeAccessRoles\": [\"User\"],%n" +
                "  \"overwriteFlag\": true,%n" +
                "  \"operationChain\": {\"operations\": [{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\", \"input\": [{\"vertex\": \"seed\", \"class\": \"uk.gov.gchq.gaffer.operation.data.EntitySeed\"}]}]}" +
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
            opChain = new String(serialiser.serialise(OPERATION_CHAIN));
        } catch (SerialisationException e) {
            fail();
        }
        assertEquals(opChain, addNamedOperation.getOperationChainAsString());
        assertEquals("Test", addNamedOperation.getOperationName());
        assertEquals("Test Named Operation", addNamedOperation.getDescription());
        assertEquals(Arrays.asList(USER), addNamedOperation.getReadAccessRoles());
        assertEquals(Arrays.asList(USER), addNamedOperation.getWriteAccessRoles());
    }

    @Override
    protected AddNamedOperation getTestObject() {
        return new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .build();
    }
}
