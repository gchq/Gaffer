
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
package gaffer.graph.hook;


import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.exception.UnauthorisedException;
import gaffer.operation.OperationChain;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetAllEdges;
import gaffer.user.User;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class OperationChainLimiterTest {

    private static final OperationChainLimiter OPERATION_CHAIN_LIMITER = new OperationChainLimiter(StreamUtil.opScores(OperationChainLimiterTest.class), StreamUtil.roleScores(OperationChainLimiterTest.class));

    @Test
     public void shouldAcceptOperationChainWhenUserHasRoleScoreGreaterThanChainScore() {
        // Given
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .build();
        final User user = new User.Builder()
                .opAuths("User")
                .build();

        // When
        OPERATION_CHAIN_LIMITER.preExecute(opChain, user);

        // Then - no exceptions
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasRoleScoreEqualToChainScore() {
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
    public void shouldRejectOperationChainWhenUserHasRoleScoreLessThanChainScore() {
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
    public void shouldAcceptOperationChainWhenUserHasMaxRoleScoreGreaterThanChainScore() {
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
    public void shouldRejectOperationChainWhenUserHasMaxRoleScoreLessThanChainScore() {
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
    public void shouldRejectOperationChainWhenUserHasNoRoleWithAConfiguredScore() {
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
}