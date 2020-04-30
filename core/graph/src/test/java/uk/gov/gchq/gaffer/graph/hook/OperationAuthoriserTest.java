/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import com.google.common.collect.Sets;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.TestOperationsImpl;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class OperationAuthoriserTest extends GraphHookTest<OperationAuthoriser> {

    private static final String OP_AUTHS_PATH = "/opAuthoriser.json";

    public OperationAuthoriserTest() {
        super(OperationAuthoriser.class);
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasAllOpAuths() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetElements())
                .then(new GenerateObjects<>())
                .then(new DiscardOutput())
                .then(new TestOperationsImpl(Collections.singletonList(new Sort())))
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "ReadUser", "User")
                .build();

        assertDoesNotThrow(() -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllOpAuthsForNestedOperations() {
        // Given
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetElements())
                .then(new GenerateObjects<>())
                .then(new DiscardOutput())
                .then(new TestOperationsImpl(Collections.singletonList(new GetAllElements())))
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "ReadUser", "User")
                .build();

        // When/Then
        assertThrows(UnauthorisedException.class, () -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasAllOpAuthsForAddNamedOperation() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final AddNamedOperation addNamedOperation = makeAddNamedOperation("{\"operations\":[{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetElements\", \"options\": {\"optionKey\": \"${testParameter}\"}}]}");
        final OperationChain opChain = new OperationChain.Builder()
                .first(addNamedOperation)
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "ReadUser", "User")
                .build();

        assertDoesNotThrow(() -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllOpAuthsForAddNamedOperation() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final AddNamedOperation addNamedOperation = makeAddNamedOperation("{\"operations\":[{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\", \"options\": {\"optionKey\": \"${testParameter}\"}}]}");
        final OperationChain opChain = new OperationChain.Builder()
                .first(addNamedOperation)
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "ReadUser", "User")
                .build();

        assertThrows(UnauthorisedException.class, () -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveSuperAuthForAddNamedOperation() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final AddNamedOperation addNamedOperation = makeAddNamedOperation("{\"operations\":[{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\", \"options\": {\"optionKey\": \"${testParameter}\"}}]}");
        final OperationChain opChain = new OperationChain.Builder()
                .first(addNamedOperation)
                .build();
        final User user = new User.Builder()
                .opAuths("ReadUser", "User")
                .build();

        assertThrows(UnauthorisedException.class, () -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveWriteAuthForAddNamedOperation() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final AddNamedOperation addNamedOperation = makeAddNamedOperation("{\"operations\":[{\"class\": \"uk.gov.gchq.gaffer.operation.impl.get.AddElements\", \"options\": {\"optionKey\": \"${testParameter}\"}}]}");
        final OperationChain opChain = new OperationChain.Builder()
                .first(addNamedOperation)
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        assertThrows(UnauthorisedException.class, () -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllOpAuthsForAllOperations() {
        // Given
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds())  // Requires SuperUser
                .build();

        final User user = new User.Builder()
                .opAuths("WriteUser", "ReadUser", "User")
                .build();

        // When/Then
        assertThrows(UnauthorisedException.class, () -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAnyOpAuths() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds())
                .then(new GetElements())
                .build();

        final User user = new User();

        assertThrows(UnauthorisedException.class, () -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllowedAuth() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds())
                .then(new GetElements())
                .build();

        final User user = new User.Builder()
                .opAuths("unknownAuth")
                .build();

        assertThrows(UnauthorisedException.class, () -> hook.preExecute(opChain, new Context(user)));
    }

    @Test
    public void shouldReturnAllOpAuths() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);

        final Set<String> allOpAuths = hook.getAllAuths();

        final Matcher<Iterable<String>> matcher = IsCollectionContaining.hasItems("User", "ReadUser", "WriteUser", "SuperUser", "AdminUser");
        assertThat(allOpAuths, matcher);
    }

    @Test
    public void shouldReturnResultWithoutModification() {
        final OperationAuthoriser hook = fromJson(OP_AUTHS_PATH);
        final Object result = mock(Object.class);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("NoScore")
                .build();

        final Object returnedResult = hook.postExecute(result, opChain, new Context(user));

        assertSame(result, returnedResult);
    }

    @Test
    public void shouldSetAndGetAuths() {
        // Given
        final OperationAuthoriser hook = new OperationAuthoriser();
        final Map<Class<?>, Set<String>> auths = new HashMap<>();
        auths.put(Operation.class, Sets.newHashSet("auth1"));
        auths.put(GetElements.class, Sets.newHashSet("auth2"));
        auths.put(GetAllElements.class, Sets.newHashSet("auth3", "auth4"));

        // When
        hook.setAuths(auths);
        final Map<Class<?>, Set<String>> result = hook.getAuths();

        // Then
        assertEquals(auths, result);

        final HashSet<String> expected = Sets.newHashSet("auth1", "auth2", "auth3", "auth4");
        assertEquals(expected, hook.getAllAuths());
    }

    @Test
    public void shouldSetAndGetAuthsAsStrings() throws ClassNotFoundException {
        // Given
        final OperationAuthoriser hook = new OperationAuthoriser();
        final Map<String, Set<String>> auths = new HashMap<>();
        auths.put(Operation.class.getName(), Sets.newHashSet("auth1"));
        auths.put(GetElements.class.getName(), Sets.newHashSet("auth2"));
        auths.put(GetAllElements.class.getName(), Sets.newHashSet("auth3", "auth4"));

        // When
        hook.setAuthsFromStrings(auths);
        final Map<String, Set<String>> result = hook.getAuthsAsStrings();

        // Then
        assertEquals(auths, result);

        final HashSet<String> expected = Sets.newHashSet("auth1", "auth2", "auth3", "auth4");
        assertEquals(expected, hook.getAllAuths());
    }

    @Override
    protected OperationAuthoriser getTestObject() {
        return fromJson(OP_AUTHS_PATH);
    }

    private AddNamedOperation makeAddNamedOperation(final String operationChain) {
        return new AddNamedOperation.Builder()
                .operationChain(operationChain)
                .description("Test Named Operation")
                .name("Test")
                .overwrite(false)
                .parameter("testParameter", new ParameterDetail.Builder()
                        .description("the seed")
                        .defaultValue("seed1")
                        .valueClass(String.class)
                        .required(false)
                        .build())
                .score(2)
                .build();
    }
}
