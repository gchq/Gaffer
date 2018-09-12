/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.ValidateOperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ValidateOperationChainHandlerTest {

    private final Store store = mock(Store.class);
    private final Context context = new Context(new User());

    @Test
    public void shouldValidateOperationChain() throws OperationException {
        // Given
        final AddElements addElements = new AddElements();
        final GetAdjacentIds getAdj = new GetAdjacentIds();
        final GetElements getElements = new GetElements();
        final DiscardOutput discardOutput = new DiscardOutput();
        OperationChain chain = new OperationChain.Builder().first(addElements).then(getAdj).then(getElements).then(discardOutput).build();
        ValidateOperationChain validateOperationChain = new ValidateOperationChain.Builder().operationChain(chain).build();

        given(store.getOperationChainValidator()).willReturn(new OperationChainValidator(new ViewValidator()));
        ValidateOperationChainHandler handler = new ValidateOperationChainHandler();

        // When
        ValidationResult result = handler.doOperation(validateOperationChain, context, store);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldReturnValidationResultWithErrorsIfOperationChainInvalid() throws OperationException {
        // Given
        final AddElementsFromSocket addElementsFromSocket = new AddElementsFromSocket();

        OperationChain chain = new OperationChain.Builder().first(addElementsFromSocket).build();
        ValidateOperationChain validateOperationChain = new ValidateOperationChain.Builder().operationChain(chain).build();

        given(store.getOperationChainValidator()).willReturn(new OperationChainValidator(new ViewValidator()));
        ValidateOperationChainHandler handler = new ValidateOperationChainHandler();

        // When
        ValidationResult result = handler.doOperation(validateOperationChain, context, store);

        // Then
        assertFalse(result.isValid());
        assertTrue(result.getErrorString().contains("elementGenerator is required for: AddElementsFromSocket"));
        assertTrue(result.getErrorString().contains("hostname is required for: AddElementsFromSocket"));
    }
}
