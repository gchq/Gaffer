/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedOperationHandlerTest {
    private static final String TEST_GRAPH_ID = "testGraphId";
    private User user;
    private Context context;

    @BeforeEach
    public void setUp() throws Exception {
        user = testUser();
        context = new Context(user);
    }

    @Test
    public final void shouldMergeResultsFromFieldObjects() throws Exception {
        // Given
        final Operation op = mock(Operation.class);
        final Operation opClone = mock(Operation.class);
        given(op.shallowClone()).willReturn(opClone);
        given(opClone.shallowClone()).willReturn(opClone);
        final OperationChain<?> opChainClone = OperationChain.wrap(opClone);
        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();
        Store mockStore1 = getMockStore(unusedSchema, storeProperties);
        Store mockStore2 = getMockStore(unusedSchema, storeProperties);
        Store mockStore3 = getMockStore(unusedSchema, storeProperties);
        Store mockStore4 = getMockStore(unusedSchema, storeProperties);

        Graph graph1 = getGraphWithMockStore(mockStore1);
        Graph graph2 = getGraphWithMockStore(mockStore2);
        Graph graph3 = getGraphWithMockStore(mockStore3);
        Graph graph4 = getGraphWithMockStore(mockStore4);

        FederatedStore mockStore = mock(FederatedStore.class);
        LinkedHashSet<Graph> linkedGraphs = Sets.newLinkedHashSet();
        linkedGraphs.add(graph1);
        linkedGraphs.add(graph2);
        linkedGraphs.add(graph3);
        linkedGraphs.add(graph4);
        when(mockStore.getGraphs(user, null, op)).thenReturn(linkedGraphs);

        // When
        new FederatedOperationHandler().doOperation(op, context, mockStore);

        verify(mockStore1).execute(eq(opChainClone), any(Context.class));
        verify(mockStore2).execute(eq(opChainClone), any(Context.class));
        verify(mockStore3).execute(eq(opChainClone), any(Context.class));
        verify(mockStore4).execute(eq(opChainClone), any(Context.class));
    }

    @Test
    public final void shouldMergeResultsFromFieldObjectsWithGivenGraphIds() throws Exception {
        // Given
        final Operation op = mock(Operation.class);
        final Operation opClone = mock(Operation.class);
        given(op.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS)).willReturn("1,3");
        given(op.shallowClone()).willReturn(opClone);
        given(opClone.shallowClone()).willReturn(opClone);

        final OperationChain<?> opChainClone = OperationChain.wrap(opClone);

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();
        Store mockStore1 = getMockStore(unusedSchema, storeProperties);
        Store mockStore2 = getMockStore(unusedSchema, storeProperties);
        Store mockStore3 = getMockStore(unusedSchema, storeProperties);
        Store mockStore4 = getMockStore(unusedSchema, storeProperties);

        Graph graph1 = getGraphWithMockStore(mockStore1);
        Graph graph3 = getGraphWithMockStore(mockStore3);

        FederatedStore mockStore = mock(FederatedStore.class);
        LinkedHashSet<Graph> filteredGraphs = Sets.newLinkedHashSet();
        filteredGraphs.add(graph1);
        filteredGraphs.add(graph3);
        when(mockStore.getGraphs(user, "1,3", op)).thenReturn(filteredGraphs);

        // When
        new FederatedOperationHandler().doOperation(op, context, mockStore);

        verify(mockStore1).execute(eq(opChainClone), any(Context.class));
        verify(mockStore2, never()).execute(eq(opChainClone), any(Context.class));
        verify(mockStore3).execute(eq(opChainClone), any(Context.class));
        verify(mockStore4, never()).execute(eq(opChainClone), any(Context.class));
    }

    private Graph getGraphWithMockStore(final Store mockStore) {
        return new Graph.Builder()
                .config(new GraphConfig(TEST_GRAPH_ID))
                .store(mockStore)
                .build();
    }

    private Store getMockStore(final Schema unusedSchema, final StoreProperties storeProperties) {
        Store mockStore1 = mock(Store.class);
        given(mockStore1.getSchema()).willReturn(unusedSchema);
        given(mockStore1.getProperties()).willReturn(storeProperties);
        return mockStore1;
    }

    @Test
    public void shouldThrowException() throws Exception {
        String message = "test exception";
        final Operation op = mock(Operation.class);
        final String graphID = "1,3";
        given(op.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS)).willReturn(graphID);
        given(op.shallowClone()).willReturn(op);

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStoreInner = getMockStore(unusedSchema, storeProperties);
        given(mockStoreInner.execute(any(OperationChain.class), any(Context.class))).willThrow(new RuntimeException(message));

        FederatedStore mockStore = mock(FederatedStore.class);
        HashSet<Graph> filteredGraphs = Sets.newHashSet(getGraphWithMockStore(mockStoreInner));
        when(mockStore.getGraphs(user, graphID, op)).thenReturn(filteredGraphs);
        try {
            new FederatedOperationHandler().doOperation(op, context, mockStore);
            fail("Exception Not thrown");
        } catch (OperationException e) {
            assertEquals(message, e.getCause().getMessage());
        }

    }

    @Test
    public void shouldNotThrowExceptionBecauseSkipFlagSetTrue() throws Exception {
        // Given
        final String graphID = "1,3";
        final Operation op = mock(Operation.class);
        when(op.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS)).thenReturn(graphID);
        when(op.getOption(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE)).thenReturn(String.valueOf(true));
        when(op.getOption(eq(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE), any(String.class))).thenReturn(String.valueOf(true));
        given(op.shallowClone()).willReturn(op);

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore1 = getMockStore(unusedSchema, storeProperties);
        given(mockStore1.execute(any(OperationChain.class), eq(context))).willReturn(1);
        Store mockStore2 = getMockStore(unusedSchema, storeProperties);
        given(mockStore2.execute(any(OperationChain.class), eq(context))).willThrow(new RuntimeException("Test Exception"));

        FederatedStore mockStore = mock(FederatedStore.class);
        LinkedHashSet<Graph> filteredGraphs = Sets.newLinkedHashSet();
        filteredGraphs.add(getGraphWithMockStore(mockStore1));
        filteredGraphs.add(getGraphWithMockStore(mockStore2));
        when(mockStore.getGraphs(user, graphID, op)).thenReturn(filteredGraphs);

        // When
        try {
            new FederatedOperationHandler().doOperation(op, context, mockStore);
        } catch (Exception e) {
            fail("Exception should not have been thrown: " + e.getMessage());
        }

        // Then
        final ArgumentCaptor<Context> contextCaptor1 = ArgumentCaptor.forClass(Context.class);
        verify(mockStore1, atLeastOnce()).execute(any(OperationChain.class), contextCaptor1.capture());
        assertEquals(context.getUser(), contextCaptor1.getValue().getUser());
        assertNotEquals(context.getJobId(), contextCaptor1.getValue().getJobId());

        final ArgumentCaptor<Context> contextCaptor2 = ArgumentCaptor.forClass(Context.class);
        verify(mockStore2, atLeastOnce()).execute(any(OperationChain.class), contextCaptor2.capture());
        assertEquals(context.getUser(), contextCaptor2.getValue().getUser());
        assertNotEquals(context.getJobId(), contextCaptor2.getValue().getJobId());
    }

    @Test
    public final void shouldPassGlobalsOnToSubstores() throws Exception {
        // Given
        final ElementFilter filter = mock(ElementFilter.class);

        final GlobalViewElementDefinition globalEntitiesAggregate = new GlobalViewElementDefinition.Builder()
                .postAggregationFilter(filter)
                .build();
        final View view = new View.Builder()
                .globalEntities(globalEntitiesAggregate)
                .build();

        final Operation operation = new GetElements.Builder()
                .input("input")
                .view(view)
                .build();
        final OperationChain op = new OperationChain.Builder()
                .first(operation)
                .build();

        Schema unusedSchema = new Schema.Builder().build();

        final Schema concreteSchema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .aggregate(false)
                        .build())
                .entity(TestGroups.ENTITY + "2", new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .aggregate(false)
                        .build())
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())

                .build();

        StoreProperties storeProperties = new StoreProperties();
        Store mockStore1 = getMockStore(unusedSchema, storeProperties);
        Store mockStore2 = getMockStore(concreteSchema, storeProperties);

        Graph graph1 = getGraphWithMockStore(mockStore1);
        Graph graph2 = getGraphWithMockStore(mockStore2);

        FederatedStore mockStore = mock(FederatedStore.class);
        LinkedHashSet<Graph> linkedGraphs = Sets.newLinkedHashSet();
        linkedGraphs.add(graph1);
        linkedGraphs.add(graph2);

        when(mockStore.getGraphs(user, null, op)).thenReturn(linkedGraphs);

        final ArgumentCaptor<OperationChain> capturedOperation = ArgumentCaptor.forClass(OperationChain.class);

        // When
        new FederatedOperationHandler().doOperation(op, context, mockStore);

        verify(mockStore2).execute(capturedOperation.capture(), any(Context.class));

        assertEquals(1, capturedOperation.getAllValues().size());
        final OperationChain transformedOpChain = capturedOperation.getAllValues().get(0);
        assertEquals(1, transformedOpChain.getOperations().size());
        assertEquals(GetElements.class, transformedOpChain.getOperations().get(0).getClass());
        final View mergedView = ((GetElements) transformedOpChain.getOperations().get(0)).getView();
        assertTrue(mergedView.getGlobalEntities() == null);
        assertEquals(2, mergedView.getEntities().size());
        assertTrue(mergedView.getEntities().entrySet().stream().allMatch(x -> x.getValue().getPostAggregationFilter() != null));

    }

}
