
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
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class OperationChainLimiterTest {

    private static final OperationChainLimiter OPERATION_CHAIN_LIMITER = new OperationChainLimiter(StreamUtil.opScores(OperationChainLimiterTest.class), StreamUtil
            .authScores(OperationChainLimiterTest.class));

    @Test
    public void shouldAcceptOperationChainWhenUserHasAuthScoreGreaterThanChainScore() {
        // Given
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetEntities())
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        // When
        OPERATION_CHAIN_LIMITER.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasAuthScoreEqualToChainScore() {
        // Given
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GenerateObjects())
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        // When
        OPERATION_CHAIN_LIMITER.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldRejectOperationChainWhenUserHasAuthScoreLessThanChainScore() {
        // Given
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetAdjacentEntitySeeds())
                .then(new GetAdjacentEntitySeeds())
                .then(new GetAdjacentEntitySeeds())
                .then(new GenerateObjects())
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        // When/Then

        try {
            OPERATION_CHAIN_LIMITER.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasMaxAuthScoreGreaterThanChainScore() {
        // Given
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetAdjacentEntitySeeds())
                .then(new GetAdjacentEntitySeeds())
                .then(new GenerateObjects())
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "User")
                .build();

        // When
        OPERATION_CHAIN_LIMITER.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldRejectOperationChainWhenUserHasMaxAuthScoreLessThanChainScore() {
        // Given
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new GenerateObjects())
                .build();
        final User user = new User.Builder()
                .opAuths("SuperUser", "User")
                .build();

        // When/Then
        try {
            OPERATION_CHAIN_LIMITER.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldRejectOperationChainWhenUserHasNoAuthWithAConfiguredScore() {
        // Given
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GenerateObjects())
                .build();
        final User user = new User.Builder()
                .opAuths("NoScore")
                .build();

        // When/Then
        try {
            OPERATION_CHAIN_LIMITER.preExecute(opChain, user);
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldReturnResultWithoutModification() {
        // Given
        final Object result = mock(Object.class);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("NoScore")
                .build();

        // When
        final Object returnedResult = OPERATION_CHAIN_LIMITER.postExecute(result, opChain, user);

        // Then
        assertSame(result, returnedResult);
    }
}