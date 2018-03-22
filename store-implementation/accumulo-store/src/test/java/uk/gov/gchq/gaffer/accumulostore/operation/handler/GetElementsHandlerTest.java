/*
 * Copyright 2017-2018 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class GetElementsHandlerTest {
    @Test
    public void shouldThrowExceptionIfAnOldOperationOptionIsUsed() throws OperationException, StoreException {
        // Given
        final Iterable<EntityId> ids = mock(Iterable.class);
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements getElements = new GetElements.Builder()
                .input(ids)
                .option("accumulostore.operation.return_matched_id_as_edge_source", "true")
                .build();

        // When / Then
        try {
            handler.doOperation(getElements, new Context(), null);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("return_matched_id_as_edge_source"));
        }
    }

    @Test
    public void shouldFailIfOperationInputIsUndefined() {
        // Given
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements op = new GetElements.Builder()
                .build();

        // When / Then
        try {
            handler.doOperation(op, new Context(), null);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().equals("Operation input is undefined - please specify an input."));
        }
    }

    @Test
    public void shouldNotReturnDeletedElements() {
        // Given
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements op = new GetElements.Builder()
                .build();

        // When / Then
        try {
            handler.doOperation(op, new Context(), null);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().equals("Operation input is undefined - please specify an input."));
        }
    }
}
