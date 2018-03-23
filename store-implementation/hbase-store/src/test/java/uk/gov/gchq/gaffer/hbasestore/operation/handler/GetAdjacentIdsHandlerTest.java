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
import org.mockito.ArgumentCaptor;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.retriever.HBaseRetriever;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;

public class GetAdjacentIdsHandlerTest {
    @Test
    public void shouldReturnHBaseRetriever() throws OperationException, StoreException {
        // Given
        final Iterable<EntityId> ids = mock(Iterable.class);
        final Context context = mock(Context.class);
        final User user = mock(User.class);
        final HBaseStore store = mock(HBaseStore.class);
        final HBaseRetriever<GetElements> hbaseRetriever = mock(HBaseRetriever.class);
        final GetAdjacentIdsHandler handler = new GetAdjacentIdsHandler();
        final GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .inputIds(ids)
                .option("option1", "optionValue")
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .directedType(DirectedType.DIRECTED)
                .view(new View())
                .build();

        given(context.getUser()).willReturn(user);
        final ArgumentCaptor<GetElements> getElementsCaptor = ArgumentCaptor.forClass(GetElements.class);
        given(store.createRetriever(getElementsCaptor.capture(), eq(user), eq(ids), eq(true))).willReturn(hbaseRetriever);

        // When
        final GetAdjacentIdsHandler.ExtractDestinationEntityId result =
                (GetAdjacentIdsHandler.ExtractDestinationEntityId) handler.doOperation(getAdjacentIds, context, store);

        // Then
        assertSame(hbaseRetriever, result.getInput());

        final GetElements getElements = getElementsCaptor.getValue();
        assertSame(ids, getElements.getInput());
        assertTrue(getElements.getView().getEntities().isEmpty());
        assertEquals(getAdjacentIds.getDirectedType(), getElements.getDirectedType());
        assertEquals(getAdjacentIds.getIncludeIncomingOutGoing(), getElements.getIncludeIncomingOutGoing());
        assertEquals("optionValue", getElements.getOption("option1"));
    }

    @Test
    public void shouldDoNothingIfNoSeedsProvided() throws OperationException {
        // Given
        final GetAdjacentIdsHandler handler = new GetAdjacentIdsHandler();
        final GetAdjacentIds getAdjacentIds = new GetAdjacentIds();
        final Context context = mock(Context.class);
        final HBaseStore store = mock(HBaseStore.class);

        // When
        final CloseableIterable<? extends EntityId> result = handler.doOperation(getAdjacentIds, context, store);

        // Then
        assertEquals(0, Iterables.size(result));
    }
}
