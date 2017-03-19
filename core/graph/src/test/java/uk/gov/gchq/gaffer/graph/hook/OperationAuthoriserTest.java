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

package uk.gov.gchq.gaffer.graph.hook;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;


public class OperationAuthoriserTest {
    @Test
    public void shouldAcceptOperationChainWhenUserHasAllOpAuths() {
        // Given
        final OperationAuthoriser opAuthoriser = new OperationAuthoriser(StreamUtil.opAuths(getClass()));
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GenerateObjects())
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "ReadUser", "User")
                .build();

        // When
        opAuthoriser.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllOpAuthsForAllOperations() {
        // Given
        final OperationAuthoriser opAuthoriser = new OperationAuthoriser(StreamUtil.opAuths(getClass()));
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GenerateObjects()) // Requires AdminUser
                .build();

        final User user = new User.Builder()
                .opAuths("WriteUser", "ReadUser", "User")
                .build();

        // When/Then
        try {
            opAuthoriser.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAnyOpAuths() throws OperationException {
        // Given
        final OperationAuthoriser opAuthoriser = new OperationAuthoriser(StreamUtil.opAuths(getClass()));
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetElements())
                .build();

        final User user = new User();

        // When/Then
        try {
            opAuthoriser.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllowedAuth() throws OperationException {
        // Given
        final OperationAuthoriser opAuthoriser = new OperationAuthoriser(StreamUtil.opAuths(getClass()));
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetElements())
                .build();

        final User user = new User.Builder()
                .opAuths("unknownAuth")
                .build();

        // When/Then
        try {
            opAuthoriser.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldReturnAllOpAuths() {
        // Given
        final OperationAuthoriser opAuthoriser = new OperationAuthoriser(StreamUtil.opAuths(getClass()));

        // When
        final Set<String> allOpAuths = opAuthoriser.getAllOpAuths();

        // Then
        assertThat(allOpAuths,
                IsCollectionContaining.hasItems("User", "ReadUser", "WriteUser", "SuperUser", "AdminUser"));
    }

    @Test
    public void shouldReturnResultWithoutModification() {
        // Given
        final OperationAuthoriser opAuthoriser = new OperationAuthoriser(StreamUtil.opAuths(getClass()));
        final Object result = mock(Object.class);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("NoScore")
                .build();

        // When
        final Object returnedResult = opAuthoriser.postExecute(result, opChain, user);

        // Then
        assertSame(result, returnedResult);
    }
}
