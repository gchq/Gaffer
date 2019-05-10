/*
 * Copyright 2017-2019 Crown Copyright
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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.LinkedHashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public abstract class FederatedOperationOutputHandlerTest<OP extends Output<O>, O> {
    public static final String TEST_ENTITY = "TestEntity";
    public static final String TEST_GRAPH_ID = "testGraphId";
    public static final String PROPERTY_TYPE = "property";
    protected O o1;
    protected O o2;
    protected O o3;
    protected O o4;
    protected User user;
    protected Context context;

    @Before
    public void setUp() throws Exception {
        user = testUser();
        context = new Context(user);
    }

    @Test
    public void shouldBeSetUp() throws Exception {
        Assert.assertNotNull("Required field object o1 is null", o1);
        Assert.assertNotNull("Required field object o2 is null", o2);
        Assert.assertNotNull("Required field object o3 is null", o3);
        Assert.assertNotNull("Required field object o4 is null", o4);
    }

    protected abstract FederatedOperationOutputHandler<OP, O> getFederatedHandler();

    protected abstract OP getExampleOperation();

    @Test
    public void shouldMergeResultsFromFieldObjects() throws Exception {
        // Given
        final OP op = getExampleOperation();

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore1 = getMockStore(unusedSchema, storeProperties, o1);
        Store mockStore2 = getMockStore(unusedSchema, storeProperties, o2);
        Store mockStore3 = getMockStore(unusedSchema, storeProperties, o3);
        Store mockStore4 = getMockStore(unusedSchema, storeProperties, o4);

        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        LinkedHashSet<Graph> linkedGraphs = Sets.newLinkedHashSet();
        linkedGraphs.add(getGraphWithMockStore(mockStore1));
        linkedGraphs.add(getGraphWithMockStore(mockStore2));
        linkedGraphs.add(getGraphWithMockStore(mockStore3));
        linkedGraphs.add(getGraphWithMockStore(mockStore4));
        Mockito.when(mockStore.getGraphs(user, null)).thenReturn(linkedGraphs);

        // When
        O theMergedResultsOfOperation = getFederatedHandler().doOperation(op, context, mockStore);

        //Then
        validateMergeResultsFromFieldObjects(theMergedResultsOfOperation, o1, o2, o3, o4);
        verify(mockStore1).execute(any(OperationChain.class), any(Context.class));
        verify(mockStore2).execute(any(OperationChain.class), any(Context.class));
        verify(mockStore3).execute(any(OperationChain.class), any(Context.class));
        verify(mockStore4).execute(any(OperationChain.class), any(Context.class));
    }

    @Test
    public void shouldMergeResultsFromFieldObjectsWithGivenGraphIds() throws Exception {
        // Given
        final OP op = getExampleOperation();
        op.addOption(KEY_OPERATION_OPTIONS_GRAPH_IDS, "1,3");

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore1 = getMockStore(unusedSchema, storeProperties, o1);
        Store mockStore2 = getMockStore(unusedSchema, storeProperties, o2);
        Store mockStore3 = getMockStore(unusedSchema, storeProperties, o3);
        Store mockStore4 = getMockStore(unusedSchema, storeProperties, o4);

        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        LinkedHashSet<Graph> filteredGraphs = Sets.newLinkedHashSet();
        filteredGraphs.add(getGraphWithMockStore(mockStore1));
        filteredGraphs.add(getGraphWithMockStore(mockStore3));
        Mockito.when(mockStore.getGraphs(user, "1,3")).thenReturn(filteredGraphs);

        // When
        O theMergedResultsOfOperation = getFederatedHandler().doOperation(op, context, mockStore);

        //Then
        validateMergeResultsFromFieldObjects(theMergedResultsOfOperation, o1, o3);
        verify(mockStore1).execute(any(OperationChain.class), any(Context.class));
        verify(mockStore2, never()).execute(any(OperationChain.class), any(Context.class));
        verify(mockStore3).execute(any(OperationChain.class), any(Context.class));
        verify(mockStore4, never()).execute(any(OperationChain.class), any(Context.class));
    }

    @Test
    public void shouldThrowException() throws Exception {
        // Given
        final String message = "Test Exception";
        final OP op = getExampleOperation();
        op.addOption(KEY_OPERATION_OPTIONS_GRAPH_IDS, TEST_GRAPH_ID);

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStoreInner = Mockito.mock(Store.class);
        given(mockStoreInner.getSchema()).willReturn(unusedSchema);
        given(mockStoreInner.getProperties()).willReturn(storeProperties);
        given(mockStoreInner.execute(any(OperationChain.class), any(Context.class))).willThrow(new RuntimeException(message));
        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        HashSet<Graph> filteredGraphs = Sets.newHashSet(getGraphWithMockStore(mockStoreInner));
        Mockito.when(mockStore.getGraphs(user, TEST_GRAPH_ID)).thenReturn(filteredGraphs);

        // When
        try {
            getFederatedHandler().doOperation(op, context, mockStore);
            fail("Exception not thrown");
        } catch (OperationException e) {
            assertEquals(message, e.getCause().getMessage());
        }
    }

    @Test
    public void shouldReturnEmptyIterableWhenNoResults() throws Exception {
        // Given
        final OP op = getExampleOperation();
        op.addOption(KEY_OPERATION_OPTIONS_GRAPH_IDS, TEST_GRAPH_ID);

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStoreInner = Mockito.mock(Store.class);
        given(mockStoreInner.getSchema()).willReturn(unusedSchema);
        given(mockStoreInner.getProperties()).willReturn(storeProperties);
        given(mockStoreInner.execute(any(OperationChain.class), eq(context))).willReturn(null);
        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        HashSet<Graph> filteredGraphs = Sets.newHashSet(getGraphWithMockStore(mockStoreInner));
        Mockito.when(mockStore.getGraphs(user, TEST_GRAPH_ID)).thenReturn(filteredGraphs);

        // When
        final O results = getFederatedHandler().doOperation(op, context, mockStore);

        assertEquals(0, Iterables.size((Iterable) results));
    }

    @Test
    public void shouldNotThrowException() throws Exception {
        // Given
        // Given
        final OP op = getExampleOperation();
        op.addOption(KEY_OPERATION_OPTIONS_GRAPH_IDS, "1,3");
        op.addOption(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE, String.valueOf(true));

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore1 = getMockStore(unusedSchema, storeProperties, o1);
        Store mockStore2 = getMockStore(unusedSchema, storeProperties, o2);
        Store mockStore3 = Mockito.mock(Store.class);
        given(mockStore3.getSchema()).willReturn(unusedSchema);
        given(mockStore3.getProperties()).willReturn(storeProperties);
        given(mockStore3.execute(any(OperationChain.class), eq(context))).willThrow(new RuntimeException("Test Exception"));
        Store mockStore4 = getMockStore(unusedSchema, storeProperties, o4);

        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        LinkedHashSet<Graph> filteredGraphs = Sets.newLinkedHashSet();
        filteredGraphs.add(getGraphWithMockStore(mockStore1));
        filteredGraphs.add(getGraphWithMockStore(mockStore3));
        Mockito.when(mockStore.getGraphs(user, "1,3")).thenReturn(filteredGraphs);

        // When
        O theMergedResultsOfOperation = null;
        try {
            theMergedResultsOfOperation = getFederatedHandler().doOperation(op, context, mockStore);
        } catch (Exception e) {
            fail("Exception should not have been thrown: " + e.getMessage());
        }

        //Then
        validateMergeResultsFromFieldObjects(theMergedResultsOfOperation, o1);
        ArgumentCaptor<Context> context1Captor = ArgumentCaptor.forClass(Context.class);
        verify(mockStore1).execute(any(OperationChain.class), context1Captor.capture());
        assertNotEquals(context.getJobId(), context1Captor.getValue().getJobId());
        assertEquals(context.getUser(), context1Captor.getValue().getUser());
        verify(mockStore2, never()).execute(any(OperationChain.class), any(Context.class));
        ArgumentCaptor<Context> context3Captor = ArgumentCaptor.forClass(Context.class);
        verify(mockStore3).execute(any(OperationChain.class), context3Captor.capture());
        assertNotEquals(context.getJobId(), context3Captor.getValue().getJobId());
        assertEquals(context.getUser(), context3Captor.getValue().getUser());
        verify(mockStore4, never()).execute(any(OperationChain.class), any(Context.class));
    }

    protected abstract boolean validateMergeResultsFromFieldObjects(final O result, final Object... resultParts);

    private Graph getGraphWithMockStore(final Store mockStore) {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(TEST_GRAPH_ID)
                        .build())
                .store(mockStore)
                .build();
    }


    private Store getMockStore(final Schema unusedSchema, final StoreProperties storeProperties, final O willReturn) throws uk.gov.gchq.gaffer.operation.OperationException {
        Store mockStore1 = Mockito.mock(Store.class);
        given(mockStore1.getSchema()).willReturn(unusedSchema);
        given(mockStore1.getProperties()).willReturn(storeProperties);
        given(mockStore1.execute(any(OperationChain.class), any(Context.class))).willReturn(willReturn);
        return mockStore1;
    }


}
