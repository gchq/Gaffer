/*
 * Copyright 2016-2019 Crown Copyright
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class AddNamedOperationTest extends OperationTest<AddNamedOperation> {
    public static final String USER = "User";
    private static final OperationChain OPERATION_CHAIN = new OperationChain.Builder().first(new GetAdjacentIds.Builder().input(new EntitySeed("seed")).build()).build();

    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        //Given
        List options = Arrays.asList("option1", "option2", "option3");
        Map<String, ParameterDetail> parameters = new HashMap<>();
        parameters.put("testOption", new ParameterDetail("Description", String.class, false, "On", options));

        final AddNamedOperation obj = new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .description("Test Named Operation")
                .name("Test")
                .overwrite()
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .parameters(parameters)
                .score(0)
                .build();

        // When
        final byte[] json = toJson(obj);
        final AddNamedOperation deserialisedObj = fromJson(json);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                " \"class\" : \"uk.gov.gchq.gaffer.named.operation.AddNamedOperation\",%n" +
                " \"operationName\": \"Test\",%n" +
                " \"description\": \"Test Named Operation\",%n" +
                " \"score\": 0,%n" +
                " \"operationChain\": {" +
                " \"operations\": [{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\", \"input\": [{\"vertex\" : \"seed\", \"class\": \"uk.gov.gchq.gaffer.operation.data.EntitySeed\"}]}]},%n" +
                " \"overwriteFlag\" : true,%n" +
                " \"parameters\" : {\"testOption\": {\"description\" :\"Description\", \"defaultValue\": \"On\", \"valueClass\": \"java.lang.String\", \"required\": false, \"options\": [\"option1\", \"option2\", \"option3\"]}},%n" +
                " \"readAccessRoles\" : [ \"User\" ],%n" +
                " \"writeAccessRoles\" : [ \"User\" ]%n" +
                "}"), new String(json));
        assertNotNull(deserialisedObj);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithNoOptions() {
        //Given
        Map<String, ParameterDetail> parameters = new HashMap<>();
        parameters.put("testOption", new ParameterDetail("Description", String.class, false, "On", null));

        final AddNamedOperation obj = new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .description("Test Named Operation")
                .name("Test")
                .overwrite()
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .parameters(parameters)
                .score(0)
                .build();

        // When
        final byte[] json = toJson(obj);
        final AddNamedOperation deserialisedObj = fromJson(json);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                " \"class\" : \"uk.gov.gchq.gaffer.named.operation.AddNamedOperation\",%n" +
                " \"operationName\": \"Test\",%n" +
                " \"description\": \"Test Named Operation\",%n" +
                " \"score\": 0,%n" +
                " \"operationChain\": {" +
                " \"operations\": [{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\", \"input\": [{\"vertex\" : \"seed\", \"class\": \"uk.gov.gchq.gaffer.operation.data.EntitySeed\"}]}]},%n" +
                " \"overwriteFlag\" : true,%n" +
                " \"parameters\" : {\"testOption\": {\"description\" :\"Description\", \"defaultValue\": \"On\", \"valueClass\": \"java.lang.String\", \"required\": false}},%n" +
                " \"readAccessRoles\" : [ \"User\" ],%n" +
                " \"writeAccessRoles\" : [ \"User\" ]%n" +
                "}"), new String(json));
        assertNotNull(deserialisedObj);
    }

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
        parameters.put("optionTestParameter", mock(ParameterDetail.class));

        AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .operationChain(OPERATION_CHAIN)
                .description("Test Named Operation")
                .name("Test")
                .overwrite(false)
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .parameters(parameters)
                .score(2)
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
        assertEquals(2, (int) clone.getScore());
        assertFalse(clone.isOverwriteFlag());
        assertEquals(Collections.singletonList(USER), clone.getReadAccessRoles());
        assertEquals(Collections.singletonList(USER), clone.getWriteAccessRoles());
        assertEquals(parameters, clone.getParameters());
        assertNotNull(clone.getParameters().get("optionTestParameter").getOptions());
    }

    @Test
    public void shouldGetOperationsWithDefaultParameters() {
        // Given
        final AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .operationChain("{\"operations\":[{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\", \"input\": [{\"vertex\": \"${testParameter}\", \"class\": \"uk.gov.gchq.gaffer.operation.data.EntitySeed\"}]}]}")
                .description("Test Named Operation")
                .name("Test")
                .overwrite(false)
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .parameter("testParameter", new ParameterDetail.Builder()
                        .description("the seed")
                        .defaultValue("seed1")
                        .valueClass(String.class)
                        .required(false)
                        .build())
                .score(2)
                .build();

        // When
        Collection<Operation> operations = addNamedOperation.getOperations();

        // Then
        assertEquals(
                Collections.singletonList(GetAdjacentIds.class),
                operations.stream().map(o -> o.getClass()).collect(Collectors.toList())
        );
        final GetAdjacentIds nestedOp = (GetAdjacentIds) operations.iterator().next();
        final List<? extends EntityId> input = Lists.newArrayList(nestedOp.getInput());
        assertEquals(Collections.singletonList(new EntitySeed("seed1")), input);
    }

    @Test
    public void shouldGetOperationsWhenNoDefaultParameter() {
        // Given
        final AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .operationChain("{\"operations\":[{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\", \"input\": [{\"vertex\": \"${testParameter}\", \"class\": \"uk.gov.gchq.gaffer.operation.data.EntitySeed\"}]}]}")
                .description("Test Named Operation")
                .name("Test")
                .overwrite(false)
                .readAccessRoles(USER)
                .writeAccessRoles(USER)
                .parameter("testParameter", new ParameterDetail.Builder()
                        .description("the seed")
                        .valueClass(String.class)
                        .required(false)
                        .build())
                .score(2)
                .build();

        // When
        Collection<Operation> operations = addNamedOperation.getOperations();

        // Then
        assertEquals(
                Collections.singletonList(GetAdjacentIds.class),
                operations.stream().map(o -> o.getClass()).collect(Collectors.toList())
        );
        final GetAdjacentIds nestedOp = (GetAdjacentIds) operations.iterator().next();
        final List<? extends EntityId> input = Lists.newArrayList(nestedOp.getInput());
        assertEquals(Collections.singletonList(new EntitySeed(null)), input);
    }

    @Override
    protected AddNamedOperation getTestObject() {
        return new AddNamedOperation();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("operations");
    }
}
