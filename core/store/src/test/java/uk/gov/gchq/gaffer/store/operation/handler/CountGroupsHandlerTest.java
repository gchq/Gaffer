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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class CountGroupsHandlerTest {
    private static final String GROUP1 = "GROUP1";
    private static final String GROUP2 = "GROUP2";

    static Iterable<Element> getElements() {
        final Entity entity1 = mock(Entity.class);
        final Entity entity2 = mock(Entity.class);
        final Entity entity3 = mock(Entity.class);
        final Entity entity4 = mock(Entity.class);

        final Edge edge1 = mock(Edge.class);
        final Edge edge2 = mock(Edge.class);
        final Edge edge3 = mock(Edge.class);
        final Edge edge4 = mock(Edge.class);

        lenient().when(entity1.getGroup()).thenReturn(GROUP1);
        lenient().when(entity2.getGroup()).thenReturn(GROUP2);
        lenient().when(entity3.getGroup()).thenReturn(GROUP1);
        lenient().when(entity4.getGroup()).thenReturn(GROUP1);

        lenient().when(edge1.getGroup()).thenReturn(GROUP1);
        lenient().when(edge2.getGroup()).thenReturn(GROUP2);
        lenient().when(edge3.getGroup()).thenReturn(GROUP2);
        lenient().when(edge4.getGroup()).thenReturn(GROUP2);

        return Arrays.asList(entity1, entity2, entity3, entity4,
                edge1, edge2, edge3, edge4);
    }

    @Test
    public void shouldReturnNoCountsIfElementsAreNull(@Mock final Store store,
                                                      @Mock final CountGroups countGroups) throws OperationException, IOException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Context context = new Context();

        given(countGroups.getInput()).willReturn(null);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertThat(counts.isLimitHit()).isFalse();
        assertThat(counts.getEntityGroups()).hasSize(0);
        assertThat(counts.getEdgeGroups()).hasSize(0);
        verify(countGroups).close();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldReturnGroupCountsWithoutLimit(@Mock final Store store,
                                                    @Mock final CountGroups countGroups) throws OperationException, IOException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Iterable elements = getElements();
        final Context context = new Context();

        given(countGroups.getLimit()).willReturn(null);
        given(countGroups.getInput()).willReturn(elements);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertThat(counts.isLimitHit()).isFalse();

        assertThat(counts.getEntityGroups()).hasSize(2);
        assertThat(counts.getEntityGroups().get(GROUP1)).isEqualTo(3);
        assertThat(counts.getEntityGroups().get(GROUP2)).isEqualTo(1);

        assertThat(counts.getEdgeGroups()).hasSize(2);
        assertThat(counts.getEdgeGroups().get(GROUP1)).isEqualTo(1);
        assertThat(counts.getEdgeGroups().get(GROUP2)).isEqualTo(3);
        verify(countGroups).close();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldReturnAllGroupCountsWhenLessThanLimit(@Mock final Store store,
                                                            @Mock final CountGroups countGroups) throws OperationException, IOException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Iterable elements = getElements();
        final Integer limit = 10;
        final Context context = new Context();

        given(countGroups.getLimit()).willReturn(limit);
        given(countGroups.getInput()).willReturn(elements);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertThat(counts.isLimitHit()).isFalse();

        assertThat(counts.getEntityGroups()).hasSize(2);
        assertThat(counts.getEntityGroups().get(GROUP1)).isEqualTo(3);
        assertThat(counts.getEntityGroups().get(GROUP2)).isEqualTo(1);

        assertThat(counts.getEdgeGroups()).hasSize(2);
        assertThat(counts.getEdgeGroups().get(GROUP1)).isEqualTo(1);
        assertThat(counts.getEdgeGroups().get(GROUP2)).isEqualTo(3);
        verify(countGroups).close();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldReturnGroupCountsUpToLimit(@Mock final Store store,
                                                 @Mock final CountGroups countGroups) throws OperationException, IOException {
        // Given
        final CountGroupsHandler handler = new CountGroupsHandler();
        final Iterable elements = getElements();
        final Integer limit = 3;
        final Context context = new Context();

        given(countGroups.getLimit()).willReturn(limit);
        given(countGroups.getInput()).willReturn(elements);

        // When
        final GroupCounts counts = handler.doOperation(countGroups, context, store);

        // Then
        assertThat(counts.isLimitHit()).isTrue();

        assertThat(counts.getEntityGroups()).hasSize(2);
        assertThat(counts.getEntityGroups().get(GROUP1)).isEqualTo(2);
        assertThat(counts.getEntityGroups().get(GROUP2)).isEqualTo(1);
        verify(countGroups).close();
    }
}
