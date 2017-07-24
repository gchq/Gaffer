
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


import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class OperationChainLimiterTest extends GraphHookTest<OperationChainLimiter> {
    private static final String OP_CHAIN_LIMITER_PATH = "opChainLimiter.json";

    public OperationChainLimiterTest() {
        super(OperationChainLimiter.class);
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasAuthScoreGreaterThanChainScore() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetElements())
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        // When
        hook.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasAuthScoreEqualToChainScore() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds())
                .then(new GetElements())
                .then(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        // When
        hook.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldRejectOperationChainWhenUserHasAuthScoreLessThanChainScore() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds())
                .then(new GetAdjacentIds())
                .then(new GetAdjacentIds())
                .then(new GetElements())
                .then(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        // When/Then

        try {
            hook.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasMaxAuthScoreGreaterThanChainScore() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds())
                .then(new GetAdjacentIds())
                .then(new GetElements())
                .then(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "User")
                .build();

        // When
        hook.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldRejectOperationChainWhenUserHasMaxAuthScoreLessThanChainScore() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new GetElements())
                .then(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "User")
                .build();

        // When/Then
        try {
            hook.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldRejectOperationChainWhenUserHasNoAuthWithAConfiguredScore() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetElements())
                .build();
        final User user = new User.Builder()
                .opAuths("NoScore")
                .build();

        // When/Then
        try {
            hook.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldReturnResultWithoutModification() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);
        final Object result = mock(Object.class);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("NoScore")
                .build();

        // When
        final Object returnedResult = hook.postExecute(result, opChain, user);

        // Then
        assertSame(result, returnedResult);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final OperationChainLimiter hook = fromJson(OP_CHAIN_LIMITER_PATH);

        // When
        final byte[] json = toJson(hook);
        final OperationChainLimiter deserialisedHook = fromJson(json);

        // Then
        assertNotNull(deserialisedHook);
    }
}
