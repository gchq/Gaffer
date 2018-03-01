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
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.operation.util.Conditional;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class IfScoreResolverTest {

    @Test
    public void shouldGetDefaultScoreWithNoOperationScores() {
        // Given
        final IfScoreResolver resolver = new IfScoreResolver();
        final DefaultScoreResolver defaultResolver = new DefaultScoreResolver(new LinkedHashMap<>());

        final If operation = new If();

        // When
        final int score = resolver.getScore(operation, defaultResolver);

        // Then
        assertEquals(1, score);
    }

    @Test
    public void shouldGetScoreWithFullyPopulatedOperation() {
        // Given
        final Count count = mock(Count.class);
        final GetAllElements getAllElements = mock(GetAllElements.class);
        final GetWalks getWalks = mock(GetWalks.class);
        final Conditional conditional = mock(Conditional.class);
        given(conditional.getTransform()).willReturn(count);

        final If operation = new If.Builder<>()
                .conditional(conditional)
                .then(getWalks)
                .otherwise(getAllElements)
                .build();

        final LinkedHashMap<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Count.class, 1);
        opScores.put(GetAllElements.class, 3);
        opScores.put(GetWalks.class, 4);

        final DefaultScoreResolver defaultResolver = new DefaultScoreResolver(opScores);

        final IfScoreResolver resolver = new IfScoreResolver();

        // When
        final int score = resolver.getScore(operation, defaultResolver);

        // Then
        assertEquals(4, score);
    }

    @Test
    public void shouldGetScoreWithOperationChainAsAnOperation() {
        // Given
        final GetElements getElements = mock(GetElements.class);
        final ToMap toMap = mock(ToMap.class);
        final uk.gov.gchq.gaffer.operation.impl.Map map = mock(uk.gov.gchq.gaffer.operation.impl.Map.class);
        final OperationChain conditionalChain = mock(OperationChain.class);
        final List<Operation> conditionalOps = new LinkedList<>();
        conditionalOps.add(getElements);
        conditionalOps.add(toMap);
        conditionalOps.add(map);

        given(conditionalChain.getOperations()).willReturn(conditionalOps);

        final Conditional conditional = mock(Conditional.class);

        given(conditional.getTransform()).willReturn(conditionalChain);

        final GetWalks getWalks = mock(GetWalks.class);
        final GetAdjacentIds getAdjacentIds = mock(GetAdjacentIds.class);

        final If operation = new If.Builder<>()
                .conditional(conditional)
                .then(getWalks)
                .otherwise(getAdjacentIds)
                .build();

        final LinkedHashMap<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class, 1);
        opScores.put(GetElements.class, 2);
        opScores.put(ToMap.class, 2);
        opScores.put(Map.class, 3);
        opScores.put(GetWalks.class, 4);
        opScores.put(GetAdjacentIds.class, 3);

        final IfScoreResolver resolver = new IfScoreResolver();
        final DefaultScoreResolver defaultResolver = new DefaultScoreResolver(opScores);

        // When
        final int score = resolver.getScore(operation, defaultResolver);

        // Then
        assertEquals(11, score);
    }

    @Test
    public void shouldGetScoreWithNestedOperations() {

    }

    @Test
    public void shouldThrowErrorWhenNoDefaultResolverConfigured() {

    }
}
