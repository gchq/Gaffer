/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.runner.argument.converter;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperationChainConverterTest {
    private static final String PATH_TO_SERIALISED_OPERATION_CHAIN = "/path";
    private ArgumentConverter argumentConverter;
    private OperationChainConverter operationChainConverter;

    @Before
    public void createOperationChainConverter() {
        argumentConverter = mock(ArgumentConverter.class);
        operationChainConverter = new OperationChainConverter(argumentConverter);
    }

    @Test
    public void shouldConvertPathToSerialisedOperationToOperationChain() {
        final GetAllElements operation = new GetAllElements.Builder()
                .directedType(DirectedType.EITHER)
                .build();
        when(argumentConverter.convert(PATH_TO_SERIALISED_OPERATION_CHAIN, Operation.class)).thenReturn(operation);
        final OperationChain resultOperationChain = operationChainConverter.convert(PATH_TO_SERIALISED_OPERATION_CHAIN);
        assertEquals(operation, resultOperationChain.getOperations().get(0));
        verify(argumentConverter).convert(PATH_TO_SERIALISED_OPERATION_CHAIN, Operation.class);
    }

    @Test
    public void shouldConvertPathToSerialisedOperationChainToOperationChain() {
        final Operation operationChain = OperationChain.wrap(
                new GetAllElements.Builder()
                        .directedType(DirectedType.EITHER)
                        .build());
        when(argumentConverter.convert(PATH_TO_SERIALISED_OPERATION_CHAIN, Operation.class)).thenReturn(operationChain);
        final OperationChain resultOperationChain = operationChainConverter.convert(PATH_TO_SERIALISED_OPERATION_CHAIN);
        assertEquals(operationChain, resultOperationChain);
    }
}
