/*
 * Copyright 2016-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedFilterHandler;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.function.FilterHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedWrappedSchema;

public class FederatedFilterHandlerTest {
    @Test
    public void shouldDelegateToHandler() throws OperationException {
        // Given
        final FederatedStore store = mock(FederatedStore.class);
        final FilterHandler handler = mock(FilterHandler.class);
        final Filter op = mock(Filter.class);
        final Context context = mock(Context.class);
        final Iterable expectedResult = mock(Iterable.class);
        final Schema schema = mock(Schema.class);

        given(store.getSchema(getFederatedWrappedSchema(), context)).willReturn(schema);
        given(handler.doOperation(op, schema)).willReturn(expectedResult);

        final FederatedFilterHandler federatedHandler = new FederatedFilterHandler(handler);

        // When
        final Object result = federatedHandler.doOperation(op, context, store);

        // Then
        assertSame(expectedResult, result);
        verify(handler).doOperation(op, schema);
        verify(store).getSchema(getFederatedWrappedSchema(), context);
    }
}
