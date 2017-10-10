/*
 * Copyright 2017 Crown Copyright
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

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.LinkedHashSet;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedAddAllElementsHandlerTest {
    protected Context context;
    protected User user;

    @Before
    public void setUp() throws Exception {
        user = testUser();
        context = new Context(user);
    }

    @Test
    public final void shouldMergeResultsFromFieldObjects() throws Exception {
        // Given
        final AddElements op = Mockito.mock(AddElements.class);
        Mockito.when(op.shallowClone()).thenReturn(op);

        Schema unusedSchema = new Schema.Builder().build();
        Store mockStore1 = getMockStore(unusedSchema);
        Store mockStore2 = getMockStore(unusedSchema);
        Store mockStore3 = getMockStore(unusedSchema);
        Store mockStore4 = getMockStore(unusedSchema);

        Graph graph1 = getGraphWithMockStore(mockStore1);
        Graph graph2 = getGraphWithMockStore(mockStore2);
        Graph graph3 = getGraphWithMockStore(mockStore3);
        Graph graph4 = getGraphWithMockStore(mockStore4);

        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        LinkedHashSet<Graph> linkedGraphs = Sets.newLinkedHashSet();
        linkedGraphs.add(graph1);
        linkedGraphs.add(graph2);
        linkedGraphs.add(graph3);
        linkedGraphs.add(graph4);
        Mockito.when(mockStore.getGraphs(user, null)).thenReturn(linkedGraphs);

        // When
        new FederatedAddElementsHandler().doOperation(op, context, mockStore);

        verify(mockStore1).execute(Mockito.eq(new OperationChain<>(op)), Mockito.any(Context.class));
        verify(mockStore2).execute(Mockito.eq(new OperationChain<>(op)), Mockito.any(Context.class));
        verify(mockStore3).execute(Mockito.eq(new OperationChain<>(op)), Mockito.any(Context.class));
        verify(mockStore4).execute(Mockito.eq(new OperationChain<>(op)), Mockito.any(Context.class));
    }


    private Graph getGraphWithMockStore(final Store mockStore) {
        return new Graph.Builder()
                .config(new GraphConfig("testGraphId"))
                .store(mockStore)
                .build();
    }


    private Store getMockStore(final Schema unusedSchema) {
        Store mockStore1 = Mockito.mock(Store.class);
        given(mockStore1.getSchema()).willReturn(unusedSchema);
        return mockStore1;
    }


}
