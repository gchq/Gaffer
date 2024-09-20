/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.optimiser;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CountAllElementsOperationChainOptimiserTest {

    private static final Operation COUNT_ALL_ELEMENTS_DEFAULT_VIEW = new CountAllElementsDefaultView.Builder().build();
    private static final Operation GET_ALL_ELEMENTS_DEFAULT_VIEW = new GetAllElements.Builder().build();
    private static final Operation COUNT = new Count<>();
    private static final Operation GET_ALL_ELEMENTS_NON_DEFAULT_VIEW = new GetAllElements.Builder().view(new View.Builder().build()).build();
    private static final Operation GET_ALL_ELEMENTS_DIRECTED = new GetAllElements.Builder().directedType(DirectedType.DIRECTED).build();
    private static final Operation GET_ALL_ELEMENTS_UNDIRECTED = new GetAllElements.Builder().directedType(DirectedType.UNDIRECTED).build();
    private static final Operation GET_ALL_ELEMENTS_EITHER_DIRECTED = new GetAllElements.Builder().directedType(DirectedType.EITHER).build();
    private static final Operation GET_ELEMENTS_DEFAULT_VIEW = new GetElements.Builder().build();
    private static final Operation COUNT_GROUPS = new CountGroups();

    @ParameterizedTest
    @MethodSource("inputOperationChainAndExpectedOptimizedOperationChain")
    void shouldReturnExpectedOptimisedOperationChain(final OperationChain inputOperationChain, final OperationChain expectedOptimisedOperationChain) {
        //Given
        final CountAllElementsOperationChainOptimiser optimiser = new CountAllElementsOperationChainOptimiser();

        // When
        final OperationChain optimisedOperationChain = optimiser.optimise(inputOperationChain);

        // Then
        assertThat(optimisedOperationChain.getOperations()).hasSameSizeAs(expectedOptimisedOperationChain.getOperations());
        final Iterator<Operation> optimisedOperationsIterator = optimisedOperationChain.getOperations().iterator();
        final Iterator<Operation> expectedOperationsIterator = expectedOptimisedOperationChain.getOperations().iterator();
        while (optimisedOperationsIterator.hasNext()) {
            assertThat(optimisedOperationsIterator.next().getClass()).isSameAs(expectedOperationsIterator.next().getClass());
        }
    }

    static Stream<Arguments> inputOperationChainAndExpectedOptimizedOperationChain() {
        return Stream.of(
                /* OperationChain's which should be optimised */
                arguments(
                        new OperationChain(GET_ALL_ELEMENTS_DEFAULT_VIEW, COUNT),
                        new OperationChain(COUNT_ALL_ELEMENTS_DEFAULT_VIEW)
                ),
                arguments(
                        new OperationChain(GET_ALL_ELEMENTS_EITHER_DIRECTED, COUNT),
                        new OperationChain(COUNT_ALL_ELEMENTS_DEFAULT_VIEW)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("nonOptimisableInputOperationChain")
    void shouldNotOptimiseOperationChain(final OperationChain inputOperationChain) {
        //Given
        final CountAllElementsOperationChainOptimiser optimiser = new CountAllElementsOperationChainOptimiser();

        // When
        final OperationChain optimisedOperationChain = optimiser.optimise(inputOperationChain);

        // Then
        assertThat(optimisedOperationChain.getOperations()).isEqualTo(inputOperationChain.getOperations());
    }

    static Stream<Arguments> nonOptimisableInputOperationChain() {
        return Stream.of(
                /* OperationChain's which should not be optimised */
                arguments(new OperationChain(GET_ALL_ELEMENTS_DEFAULT_VIEW, COUNT_GROUPS)),
                arguments(new OperationChain(GET_ELEMENTS_DEFAULT_VIEW, COUNT)),
                arguments(new OperationChain(GET_ALL_ELEMENTS_NON_DEFAULT_VIEW, COUNT)),
                arguments(new OperationChain(GET_ALL_ELEMENTS_DIRECTED, COUNT)),
                arguments(new OperationChain(GET_ALL_ELEMENTS_UNDIRECTED, COUNT))
        );
    }
}
