/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class DefaultScoreResolverTest {
    @Test
    public void shouldGetDefaultScoreWhenNoOperationScores() throws OperationException {
        // Given
        final DefaultScoreResolver resolver = new DefaultScoreResolver(new LinkedHashMap<>());

        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);

        // When
        final int score = resolver.getScore(op1);

        // Then
        assertEquals(1, score);
    }

    @Test
    public void shouldGetScore() {
        // Given
        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);

        final Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class, 1);
        opScores.put(GetAdjacentIds.class, 2);
        opScores.put(GetElements.class, 1);
        opScores.put(Limit.class, 1);
        final DefaultScoreResolver resolver = new DefaultScoreResolver(opScores);

        // When
        final int score = resolver.getScore(op1);

        // Then
        assertEquals(2, score);
    }

    @Test
    public void shouldGetScoreForOperationChain() {
        // Given
        final GetAdjacentIds getAdjacentIds = mock(GetAdjacentIds.class);
        final GetElements getElements = mock(GetElements.class);
        final Limit limit = mock(Limit.class);
        final List<Operation> opList = Arrays.asList(getAdjacentIds, getElements, limit);
        final OperationChain opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(opList);

        final Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class, 1);
        opScores.put(GetElements.class, 2);
        opScores.put(GetAdjacentIds.class, 3);
        opScores.put(Limit.class, 1);

        final DefaultScoreResolver resolver = new DefaultScoreResolver(opScores);

        // When
        final int score = resolver.getScore(opChain);

        // Then
        assertEquals(6, score);
    }

    @Test
    public void shouldGetScoreForNestedOperations() {
        // Given
        final GetElements getElements = mock(GetElements.class);
        final GetWalks getWalks = mock(GetWalks.class);
        final GetAdjacentIds getAdjacentIds = mock(GetAdjacentIds.class);
        given(getWalks.getOperations()).willReturn(
                Collections.singletonList(new OperationChain<>(getAdjacentIds, getAdjacentIds)));
        final Limit limit = mock(Limit.class);
        final List<Operation> opList = Arrays.asList(getElements, getWalks, limit);
        final OperationChain opChain = mock(OperationChain.class);
        given(opChain.getOperations()).willReturn(opList);

        final Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class, 1);
        opScores.put(GetElements.class, 2);
        opScores.put(GetAdjacentIds.class, 2);
        opScores.put(Limit.class, 1);

        final DefaultScoreResolver resolver = new DefaultScoreResolver(opScores);

        // When
        final int score = resolver.getScore(opChain);

        // Then
        assertEquals(7, score);
    }
}