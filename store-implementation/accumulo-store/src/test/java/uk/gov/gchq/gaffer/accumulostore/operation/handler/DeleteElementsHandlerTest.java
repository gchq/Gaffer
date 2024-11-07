/*
 * Copyright 2024 Crown Copyright
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
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.ValidatedElements;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

class DeleteElementsHandlerTest {

    @Test
    void shouldDoOperationValidated() throws Exception {
        // Given
        final DeleteElementsHandler handler = new DeleteElementsHandler();
        final DeleteElements op = new DeleteElements.Builder()
                .input(new Entity("entity"))
                .validate(true)
                .build();

        AccumuloStore store = Mockito.mock(AccumuloStore.class);
        handler.doOperation(op, new Context(), store);

        verify(store).deleteElements(any(ValidatedElements.class));
    }

    @Test
    void shouldDoOperationNotValidated() throws Exception {
        // Given
        final DeleteElementsHandler handler = new DeleteElementsHandler();
        final DeleteElements op = new DeleteElements.Builder()
                .input(new Entity("entity"))
                .validate(false)
                .build();

        AccumuloStore store = Mockito.mock(AccumuloStore.class);
        handler.doOperation(op, new Context(), store);

        verify(store).deleteElements(anyIterable());
    }

    @Test
    void shouldThrow() throws Exception {
        // Given
        final DeleteElementsHandler handler = new DeleteElementsHandler();
        final DeleteElements op = new DeleteElements.Builder()
                .input(new Entity("entity"))
                .build();

        AccumuloStore store = Mockito.mock(AccumuloStore.class);
        doThrow(new StoreException("Intentional Error")).when(store).deleteElements(anyIterable());

        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(op, new Context(), store))
                .withMessageContaining("Failed to delete");
    }
}
