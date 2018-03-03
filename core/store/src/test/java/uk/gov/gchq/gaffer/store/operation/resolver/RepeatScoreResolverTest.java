/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.resolver;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Repeat;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class RepeatScoreResolverTest {

    @Test
    public void shouldGetDefaultScoreWithNoOperationScores() {
        // Given
        final RepeatScoreResolver resolver = new RepeatScoreResolver();
        final DefaultScoreResolver defaultResolver = new DefaultScoreResolver(new LinkedHashMap<>());

        final Repeat repeat = new Repeat.Builder()
                .operation(new GetAdjacentIds())
                .times(2)
                .build();

        // When
        final int score = resolver.getScore(repeat, defaultResolver);

        // Then
        assertEquals(2, score);
    }

    @Test
    public void shouldGetScore() {
        // Given
        final GetAdjacentIds delegate = mock(GetAdjacentIds.class);

        final Repeat repeat = new Repeat.Builder()
                .operation(delegate)
                .times(2)
                .build();

        final RepeatScoreResolver resolver = new RepeatScoreResolver();

        final Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class, 1);
        opScores.put(GetAdjacentIds.class, 2);

        final DefaultScoreResolver defaultResolver = new DefaultScoreResolver(opScores);

        // When
        final int score = resolver.getScore(repeat, defaultResolver);

        // Then
        assertEquals(4, score);
    }

    @Test
    public void shouldGetScoreForOperationChainAsDelegate() {
        // Given
        final GetAdjacentIds delegate = mock(GetAdjacentIds.class);
        final Limit limit = mock(Limit.class);
        final OperationChain opChain = mock(OperationChain.class);
        final List<Operation> ops = Arrays.asList(delegate, limit);

        given(opChain.getOperations()).willReturn(ops);

        final Repeat repeat = new Repeat.Builder()
                .operation(opChain)
                .times(3)
                .build();

        final RepeatScoreResolver resolver = new RepeatScoreResolver();

        final Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class, 1);
        opScores.put(GetAdjacentIds.class, 3);
        opScores.put(Limit.class, 2);

        final DefaultScoreResolver defaultResolver = new DefaultScoreResolver(opScores);

        // When
        final int score = resolver.getScore(repeat, defaultResolver);

        // Then
        assertEquals(15, score);
    }

    @Test
    public void shouldThrowErrorWhenNoDefaultResolverConfigured() {
        // Given
        final RepeatScoreResolver resolver = new RepeatScoreResolver();

        final Repeat repeat = new Repeat.Builder()
                .operation(new GetAdjacentIds())
                .times(3)
                .build();

        // When / Then
        try {
            resolver.getScore(repeat);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Default score resolver has not been provided."));
        }
    }
}
