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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.mockito.Mockito;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOperationHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedOperationHandlerTest {
    private static final String TEST_GRAPH_ID = "testGraphId";
    private User testUser;
    private Context context;

    CloseableIterable<Element> output1 = new WrappedCloseableIterable<>(Lists.newArrayList(new Entity.Builder().vertex("a").build()));
    CloseableIterable<Element> output2 = new WrappedCloseableIterable<>(Lists.newArrayList(new Entity.Builder().vertex("b").build()));
    CloseableIterable<Element> output3 = new WrappedCloseableIterable<>(Lists.newArrayList(new Entity.Builder().vertex("c").build()));
    CloseableIterable<Element> output4 = new WrappedCloseableIterable<>(Lists.newArrayList(new Entity.Builder().vertex("b").build()));
    private Store mockStore1;
    private Store mockStore2;
    private Store mockStore3;
    private Store mockStore4;
    private Graph graph1;
    private Graph graph2;
    private Graph graph3;
    private Graph graph4;

    @BeforeEach
    public void setUp() throws Exception {
        testUser = testUser();
        context = new Context(testUser);

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();
        mockStore1 = getMockStoreThatAlwaysReturns(unusedSchema, storeProperties, output1);
        mockStore2 = getMockStoreThatAlwaysReturns(unusedSchema, storeProperties, output2);
        mockStore3 = getMockStoreThatAlwaysReturns(unusedSchema, storeProperties, output3);
        mockStore4 = getMockStoreThatAlwaysReturns(unusedSchema, storeProperties, output4);

        graph1 = getGraphWithMockStore(mockStore1);
        graph2 = getGraphWithMockStore(mockStore2);
        graph3 = getGraphWithMockStore(mockStore3);
        graph4 = getGraphWithMockStore(mockStore4);
    }

    //TODO FS feature other 2 types?
    private Output<CloseableIterable<? extends Element>> getPayload() {
        return new GetAllElements.Builder().build();
    }

    @Test
    public final void shouldGetAllResultsFromStores() throws Exception {
        // Given
        final Output operation = getPayload();

        FederatedStore federatedStore = mock(FederatedStore.class);

        FederatedOperation<Void, CloseableIterable<? extends Element>> federatedOperation = getFederatedOperation(operation);
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(Sets.newHashSet(graph1, graph2, graph3, graph4));

        // When
        CloseableIterable<? extends Element> results = new FederatedOperationHandler<Void, CloseableIterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore);

        assertNotNull(results);
        validateMergeResultsFromFieldObjects(results, output1, output2, output3, output4);
    }

    @Test
    public final void shouldGetAllResultsFromGraphIds() throws Exception {
        // Given
        final Output payload = getPayload();

        FederatedStore federatedStore = mock(FederatedStore.class);

        FederatedOperation<Void, CloseableIterable<? extends Element>> federatedOperation = getFederatedOperation(payload);
        federatedOperation.graphIdsCSV("1,3");
        when(federatedStore.getGraphs(testUser, "1,3", federatedOperation)).thenReturn(Sets.newHashSet(graph1, graph3));
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(Sets.newHashSet(graph1, graph2, graph3, graph4));

        // When
        CloseableIterable<? extends Element> results = new FederatedOperationHandler<Void, CloseableIterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore);

        assertNotNull(results);
        validateMergeResultsFromFieldObjects(results, output1, output3);
    }

    private Graph getGraphWithMockStore(final Store mockStore) {
        return new Graph.Builder()
                .config(new GraphConfig(TEST_GRAPH_ID))
                .store(mockStore)
                .build();
    }

    private Store getMockStoreThatAlwaysReturns(final Schema schema, final StoreProperties storeProperties, final Object willReturn) throws uk.gov.gchq.gaffer.operation.OperationException {
        Store mockStore = Mockito.mock(Store.class);
        given(mockStore.getSchema()).willReturn(schema);
        given(mockStore.getProperties()).willReturn(storeProperties);
        given(mockStore.execute(any(Output.class), any(Context.class))).willReturn(willReturn);
        return mockStore;
    }

    //TODO FS Delete ?
    @Deprecated
    private Store getMockStore(final Schema schema, final StoreProperties storeProperties) {
        Store mockStore = Mockito.mock(Store.class);
        given(mockStore.getSchema()).willReturn(schema);
        given(mockStore.getProperties()).willReturn(storeProperties);
        return mockStore;
    }

    @Test
    public void shouldThrowStoreException() throws Exception {
        // Given
        String errorMessage = "test exception";
        Store mockStore = Mockito.mock(Store.class);
        given(mockStore.getSchema()).willReturn(new Schema());
        given(mockStore.getProperties()).willReturn(new StoreProperties());
        given(mockStore.execute(any(), any())).willThrow(new RuntimeException(errorMessage));
        graph3 = getGraphWithMockStore(mockStore);

        final Output payload = getPayload();

        FederatedStore federatedStore = mock(FederatedStore.class);

        FederatedOperation<Void, CloseableIterable<? extends Element>> federatedOperation = getFederatedOperation(payload);
        federatedOperation.graphIdsCSV("1,2,3");
        when(federatedStore.getGraphs(testUser, "1,2,3", federatedOperation)).thenReturn(Sets.newHashSet(graph1, graph3));
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(Sets.newHashSet(graph1, graph2, graph3, graph4));

        // When
        try {
            CloseableIterable<? extends Element> ignore = new FederatedOperationHandler<Void, CloseableIterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore);
            fail("Exception Not thrown");
        } catch (OperationException e) {
            assertEquals(FederatedOperationHandler.ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS, e.getMessage());
            assertTrue(e.getCause().getMessage().contains(errorMessage));
        }
    }

    @Test
    public void shouldNotThrowStoreExceptionSkipFlagSetTrue() throws Exception {
        // Given
        String errorMessage = "test exception";
        Store mockStore = Mockito.mock(Store.class);
        given(mockStore.getSchema()).willReturn(new Schema());
        given(mockStore.getProperties()).willReturn(new StoreProperties());
        given(mockStore.execute(any(), any())).willThrow(new RuntimeException(errorMessage));
        graph3 = getGraphWithMockStore(mockStore);

        FederatedStore federatedStore = mock(FederatedStore.class);

        FederatedOperation<Void, CloseableIterable<? extends Element>> federatedOperation = getFederatedOperation(getPayload());
        federatedOperation.skipFailedFederatedExecution(true);
        federatedOperation.graphIdsCSV("1,2,3");
        when(federatedStore.getGraphs(testUser, "1,2,3", federatedOperation)).thenReturn(Sets.newHashSet(graph1, graph2, graph3));
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(Sets.newHashSet(graph1, graph2, graph3, graph4));

        // When
        CloseableIterable<? extends Element> results = null;

        try {
            results = new FederatedOperationHandler<Void, CloseableIterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore);
        } catch (OperationException e) {
            fail("Store with error should have been skipped.");
        }

        assertNotNull(results);
        validateMergeResultsFromFieldObjects(results, output1, output2);
    }

    @Test
    public final void shouldPassGlobalsOnToSubstores() throws Exception {
        // Given

        final Operation operation = new GetElements.Builder()
                .input("input")
                .view(new View.Builder()
                        .globalEntities(new GlobalViewElementDefinition.Builder()
                                .postAggregationFilter(mock(ElementFilter.class))
                                .build())
                        .build())
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

        when(mockStore.getGraphs(eq(testUser), eq(null), any())).thenReturn(linkedGraphs);

        final ArgumentCaptor<OperationChain> capturedOperation = ArgumentCaptor.forClass(OperationChain.class);

        // When
        new FederatedOperationHandler().doOperation(getFederatedOperation(op), context, mockStore);

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

    @Test
    public void shouldReturnEmptyOutputOfTypeIterableWhenNoResults() throws Exception {
        // Given
        Output<CloseableIterable<? extends Element>> payload = getPayload();

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore = getMockStore(unusedSchema, storeProperties);
        given(mockStore.execute(any(OperationChain.class), eq(context))).willReturn(null);

        FederatedStore federatedStore = Mockito.mock(FederatedStore.class);
        HashSet<Graph> filteredGraphs = Sets.newHashSet(getGraphWithMockStore(mockStore));
        given(federatedStore.getGraphs(eq(testUser), any(), any(FederatedOperation.class))).willReturn(filteredGraphs);

        // When
        final Object results = new FederatedOperationHandler().doOperation(getFederatedOperation(payload), context, federatedStore);

        assertNotNull(results);

        ArrayList<Element> arrayList = new ArrayList<>();
        arrayList.add(null);
        ChainedIterable<Object> expected = new ChainedIterable<>(arrayList);
        assertEquals(Lists.newArrayList((Iterable)expected), Lists.newArrayList((Iterable) results));
    }

    protected boolean validateMergeResultsFromFieldObjects(final Iterable<? extends Element> result, final CloseableIterable<? extends Element>... resultParts) {
        assertNotNull(result);
        final Iterable[] resultPartItrs = Arrays.copyOf(resultParts, resultParts.length, Iterable[].class);
        final ArrayList<Object> elements = Lists.newArrayList(new ChainedIterable<>(resultPartItrs));
        int i = 0;
        for (Element e : result) {
            assertTrue(e instanceof Entity);
            elements.contains(e);
            i++;
        }
        assertEquals(elements.size(), i);
        return true;
    }

}
