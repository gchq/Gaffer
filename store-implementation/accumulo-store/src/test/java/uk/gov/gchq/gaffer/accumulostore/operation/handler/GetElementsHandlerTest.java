/*
 * Copyright 2017-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

@ExtendWith(MockitoExtension.class)
public class GetElementsHandlerTest {

    @Test
    public void shouldThrowExceptionIfAnOldOperationOptionIsUsed(@Mock final Iterable<EntityId> mockIds) throws OperationException, StoreException {
        // Given
        final GetElementsHandler handler = new GetElementsHandler();

        final Operation operation = new GetElementsHandler.OperationBuilder()
                .id("GetElements")
                .input(mockIds)
                .operationArg("accumulostore.operation.return_matched_id_as_edge_source", "true")
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> handler.doOperation(operation, new Context(), null))
                .withMessageContaining("return_matched_id_as_edge_source");
    }

    @Test
    public void shouldFailIfOperationInputIsUndefined() {
        // Given
        final GetElementsHandler handler = new GetElementsHandler();

        final Operation operation = new GetElementsHandler.OperationBuilder()
                .id("GetElements")
                .build();

        // When / Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(operation, new Context(), null))
                .withMessageContaining("Operation input is undefined - please specify an input.");
    }

    // TODO: same as above?
    @Test
    public void shouldNotReturnDeletedElements() {
        // Given
        final GetElementsHandler handler = new GetElementsHandler();

        final Operation operation = new GetElementsHandler.OperationBuilder()
                .id("GetElements")
                .build();

        // When / Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(operation, new Context(), null))
                .withMessageContaining("Operation input is undefined - please specify an input.");
    }
}
