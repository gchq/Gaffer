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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.ElementDedupeFilterProcessor;
import uk.gov.gchq.gaffer.hbasestore.retriever.HBaseRetriever;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetAllElementsHandlerTest {
    @Test
    public void shouldReturnHBaseRetriever() throws OperationException, StoreException {
        // Given
        final Context context = mock(Context.class);
        final User user = mock(User.class);
        final HBaseStore store = mock(HBaseStore.class);
        final HBaseRetriever<GetAllElements> hbaseRetriever = mock(HBaseRetriever.class);
        final GetAllElementsHandler handler = new GetAllElementsHandler();
        final GetAllElements getElements = new GetAllElements();

        given(context.getUser()).willReturn(user);
        given(store.createRetriever(getElements, user, null, false, ElementDedupeFilterProcessor.class)).willReturn(hbaseRetriever);

        // When
        final HBaseRetriever<GetAllElements> result = (HBaseRetriever<GetAllElements>) handler.doOperation(getElements, context, store);

        // Then
        assertSame(hbaseRetriever, result);

    }
}
