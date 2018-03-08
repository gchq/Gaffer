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

import com.google.common.collect.Iterables;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.retriever.HBaseRetriever;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetElementsHandlerTest {
    @Test
    public void shouldThrowExceptionIfAnOldOperationOptionIsUsed() throws OperationException, StoreException {
        // Given
        final Iterable<EntityId> ids = mock(Iterable.class);
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements getElements = new GetElements.Builder()
                .input(ids)
                .option("hbasestore.operation.return_matched_id_as_edge_source", "true")
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
    public void shouldReturnHBaseRetrieverWithIncludeMatchedVertex() throws OperationException, StoreException {
        // Given
        final Iterable<EntityId> ids = mock(Iterable.class);
        final Context context = mock(Context.class);
        final User user = mock(User.class);
        final HBaseStore store = mock(HBaseStore.class);
        final HBaseRetriever<GetElements> hbaseRetriever = mock(HBaseRetriever.class);
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements getElements = new GetElements.Builder()
                .inputIds(ids)
                .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                .build();

        given(context.getUser()).willReturn(user);
        given(store.createRetriever(getElements, user, ids, true)).willReturn(hbaseRetriever);

        // When
        final HBaseRetriever<GetElements> result = (HBaseRetriever<GetElements>) handler.doOperation(getElements, context, store);

        // Then
        assertSame(hbaseRetriever, result);
    }

    @Test
    public void shouldReturnHBaseRetrieverWithIncludeMatchedVertexWhenSeedMatchingIsNull() throws OperationException, StoreException {
        // Given
        final Iterable<EntityId> ids = mock(Iterable.class);
        final Context context = mock(Context.class);
        final User user = mock(User.class);
        final HBaseStore store = mock(HBaseStore.class);
        final HBaseRetriever<GetElements> hbaseRetriever = mock(HBaseRetriever.class);
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements getElements = new GetElements.Builder()
                .inputIds(ids)
                .seedMatching(null)
                .build();

        given(context.getUser()).willReturn(user);
        given(store.createRetriever(getElements, user, ids, true)).willReturn(hbaseRetriever);

        // When
        final HBaseRetriever<GetElements> result = (HBaseRetriever<GetElements>) handler.doOperation(getElements, context, store);

        // Then
        assertSame(hbaseRetriever, result);
    }

    @Test
    public void shouldReturnHBaseRetrieverWithoutIncludeMatchdxVertex() throws OperationException, StoreException {
        // Given
        final Iterable<EntityId> ids = mock(Iterable.class);
        final Context context = mock(Context.class);
        final User user = mock(User.class);
        final HBaseStore store = mock(HBaseStore.class);
        final HBaseRetriever<GetElements> hbaseRetriever = mock(HBaseRetriever.class);
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements getElements = new GetElements.Builder()
                .inputIds(ids)
                .seedMatching(SeedMatching.SeedMatchingType.EQUAL)
                .build();

        given(context.getUser()).willReturn(user);
        given(store.createRetriever(getElements, user, ids, false)).willReturn(hbaseRetriever);

        // When
        final HBaseRetriever<GetElements> result = (HBaseRetriever<GetElements>) handler.doOperation(getElements, context, store);

        // Then
        assertSame(hbaseRetriever, result);
    }

    @Test
    public void shouldDoNothingIfNoSeedsProvided() throws OperationException {
        // Given
        final GetElementsHandler handler = new GetElementsHandler();
        final GetElements getElements = new GetElements();
        final Context context = mock(Context.class);
        final HBaseStore store = mock(HBaseStore.class);

        // When
        final CloseableIterable<? extends Element> result = handler.doOperation(getElements, context, store);

        // Then
        assertEquals(0, Iterables.size(result));
    }
}
