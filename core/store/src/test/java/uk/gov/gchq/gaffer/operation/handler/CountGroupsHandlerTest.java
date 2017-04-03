/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.handler;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class CountGroupsHandlerTest {
    private static final String GROUP1 = "GROUP1";
    private static final String GROUP2 = "GROUP2";

    @Test
    public void shouldReturnNoCountsIfElementsAreNull() throws OperationException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Store store = mock(Store.class);
        final CountGroups countGroups = mock(CountGroups.class);
        final Context context = new Context();

        given(countGroups.getElements()).willReturn(null);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertFalse(counts.isLimitHit());
        assertEquals(0, counts.getEntityGroups().size());
        assertEquals(0, counts.getEdgeGroups().size());
    }

    @Test
    public void shouldReturnGroupCountsWithoutLimit() throws OperationException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Store store = mock(Store.class);
        final CountGroups countGroups = mock(CountGroups.class);
        final CloseableIterable<Element> elements = getElements();
        final Context context = new Context();

        given(countGroups.getLimit()).willReturn(null);
        given(countGroups.getElements()).willReturn(elements);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertFalse(counts.isLimitHit());

        assertEquals(2, counts.getEntityGroups().size());
        assertEquals(3, (int) counts.getEntityGroups().get(GROUP1));
        assertEquals(1, (int) counts.getEntityGroups().get(GROUP2));

        assertEquals(2, counts.getEdgeGroups().size());
        assertEquals(1, (int) counts.getEdgeGroups().get(GROUP1));
        assertEquals(3, (int) counts.getEdgeGroups().get(GROUP2));
    }

    @Test
    public void shouldReturnAllGroupCountsWhenLessThanLimit() throws OperationException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Store store = mock(Store.class);
        final CountGroups countGroups = mock(CountGroups.class);
        final CloseableIterable<Element> elements = getElements();
        final Integer limit = 10;
        final Context context = new Context();

        given(countGroups.getLimit()).willReturn(limit);
        given(countGroups.getElements()).willReturn(elements);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertFalse(counts.isLimitHit());

        assertEquals(2, counts.getEntityGroups().size());
        assertEquals(3, (int) counts.getEntityGroups().get(GROUP1));
        assertEquals(1, (int) counts.getEntityGroups().get(GROUP2));

        assertEquals(2, counts.getEdgeGroups().size());
        assertEquals(1, (int) counts.getEdgeGroups().get(GROUP1));
        assertEquals(3, (int) counts.getEdgeGroups().get(GROUP2));
    }

    @Test
    public void shouldReturnGroupCountsUpToLimit() throws OperationException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Store store = mock(Store.class);
        final CountGroups countGroups = mock(CountGroups.class);
        final CloseableIterable<Element> elements = getElements();
        final Integer limit = 3;
        final Context context = new Context();

        given(countGroups.getLimit()).willReturn(limit);
        given(countGroups.getElements()).willReturn(elements);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertTrue(counts.isLimitHit());

        assertEquals(2, counts.getEntityGroups().size());
        assertEquals(2, (int) counts.getEntityGroups().get(GROUP1));
        assertEquals(1, (int) counts.getEntityGroups().get(GROUP2));

    }

    static CloseableIterable<Element> getElements() {
        final Entity entity1 = mock(Entity.class);
        final Entity entity2 = mock(Entity.class);
        final Entity entity3 = mock(Entity.class);
        final Entity entity4 = mock(Entity.class);

        final Edge edge1 = mock(Edge.class);
        final Edge edge2 = mock(Edge.class);
        final Edge edge3 = mock(Edge.class);
        final Edge edge4 = mock(Edge.class);

        given(entity1.getGroup()).willReturn(GROUP1);
        given(entity2.getGroup()).willReturn(GROUP2);
        given(entity3.getGroup()).willReturn(GROUP1);
        given(entity4.getGroup()).willReturn(GROUP1);

        given(edge1.getGroup()).willReturn(GROUP1);
        given(edge2.getGroup()).willReturn(GROUP2);
        given(edge3.getGroup()).willReturn(GROUP2);
        given(edge4.getGroup()).willReturn(GROUP2);

        return new WrappedCloseableIterable<>(Arrays.asList(
                entity1, entity2, entity3, entity4,
                edge1, edge2, edge3, edge4));
    }
}