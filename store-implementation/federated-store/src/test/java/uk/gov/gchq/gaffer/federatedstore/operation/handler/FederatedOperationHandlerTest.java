/*
 * Copyright 2017-2022 Crown Copyright
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

import com.google.common.collect.Lists;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.IterableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedAccess;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federatedstore.util.ApplyViewToElementsFunction;
import uk.gov.gchq.gaffer.federatedstore.util.ConcatenateBestEffortsMergeFunction;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
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
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionIntersect;
import uk.gov.gchq.koryphe.iterable.ChainedIterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
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
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadFederatedStoreFrom;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getCleanStrings;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getDefaultMergeFunction;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getStoreConfiguredDefaultMergeFunction;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedOperationHandlerTest {
    private static final String TEST_GRAPH_ID = "testGraphId";
    Iterable<Element> output1 = singletonList(new Entity.Builder().vertex("a").build());
    Iterable<Element> output2 = singletonList(new Entity.Builder().vertex("b").build());
    Iterable<Element> output3 = singletonList(new Entity.Builder().vertex("c").build());
    Iterable<Element> output4 = singletonList(new Entity.Builder().vertex("b").build());
    private User testUser;
    private Context context;
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

    private Output<Iterable<? extends Element>> getPayload() {
        return new GetAllElements.Builder().build();
    }

    @Test
    public final void shouldGetAllResultsFromStores() throws Exception {
        // Given
        final Output operation = getPayload();

        FederatedStore federatedStore = mock(FederatedStore.class);

        FederatedOperation federatedOperation = getFederatedOperation(operation);
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(asList(graph1, graph2, graph3, graph4));
        final HashMap mockMap = mock(HashMap.class);
        given(mockMap.get(any())).willReturn(getDefaultMergeFunction());
        given(mockMap.getOrDefault(any(), any())).willReturn(getDefaultMergeFunction());
        when(federatedStore.getStoreConfiguredDefaultMergeFunctions()).thenReturn(mockMap);

        // When
        Object results = new FederatedOperationHandler<Void, Iterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore);

        assertNotNull(results);
        validateMergeResultsFromFieldObjects(results, output1, output2, output3, output4);
    }

    @Test
    public final void shouldGetDefaultedMergeForOperation() throws Exception {

        //given
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), new FederatedStoreProperties());
        federatedStore.addGraphs(new FederatedAccess.Builder().isPublic(true).build(), new GraphSerialisable.Builder().schema(loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON)).properties(new MapStoreProperties()).config(new GraphConfig("graph")).build());

        //when
        final BiFunction traitsMerge = getStoreConfiguredDefaultMergeFunction(new GetTraits(), context, null, federatedStore);
        final BiFunction graphsIdsMerge = getStoreConfiguredDefaultMergeFunction(new GetAllGraphIds(), context, null, federatedStore);
        final BiFunction getElementsMerge = getStoreConfiguredDefaultMergeFunction(new GetElements(), context, null, federatedStore);

        //then
        assertThat(traitsMerge)
                .isNotSameAs(graphsIdsMerge)
                .isNotSameAs(getElementsMerge)
                .isInstanceOf(CollectionIntersect.class);
        assertThat(graphsIdsMerge)
                .isNotSameAs(traitsMerge)
                .isNotSameAs(getElementsMerge)
                .isInstanceOf(ConcatenateBestEffortsMergeFunction.class);
        assertThat(getElementsMerge)
                .isNotSameAs(traitsMerge)
                .isNotSameAs(graphsIdsMerge)
                .isInstanceOf(ApplyViewToElementsFunction.class);
    }

    @Test
    public final void shouldGetConfiguredMergeForOperation() throws Exception {
        //given
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), new FederatedStoreProperties());
        final FederatedStore federatedStoreConfigured = loadFederatedStoreFrom("withConfigMergeMapping.json");

        //when
        final BiFunction configuredMerge = getStoreConfiguredDefaultMergeFunction(new GetTraits(), context, null, federatedStoreConfigured);
        final BiFunction defaultMerge = getStoreConfiguredDefaultMergeFunction(new GetTraits(), context, null, federatedStore);

        //then
        assertThat(defaultMerge)
                .isNotSameAs(configuredMerge)
                .isInstanceOf(CollectionIntersect.class);

        assertThat(configuredMerge)
                .isNotSameAs(defaultMerge)
                .isInstanceOf(ConcatenateBestEffortsMergeFunction.class);
    }

    @Test
    public final void shouldGetAllResultsFromGraphIds() throws Exception {
        // Given
        final Output payload = getPayload();

        FederatedStore federatedStore = mock(FederatedStore.class);

        FederatedOperation federatedOperation = getFederatedOperation(payload);
        final List<String> graphIds = asList("1", "3");
        federatedOperation.graphIds(graphIds);
        when(federatedStore.getGraphs(testUser, graphIds, federatedOperation)).thenReturn(asList(graph1, graph3));
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(asList(graph1, graph2, graph3, graph4));
        final HashMap mockMap = mock(HashMap.class);
        given(mockMap.get(any())).willReturn(getDefaultMergeFunction());
        given(mockMap.getOrDefault(any(), any())).willReturn(getDefaultMergeFunction());
        given(federatedStore.getStoreConfiguredDefaultMergeFunctions()).willReturn(mockMap);

        // When
        Object results = new FederatedOperationHandler<Void, Iterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore);

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

        FederatedOperation federatedOperation = getFederatedOperation(payload);
        final List<String> graphIds = asList("1", "2", "3");
        federatedOperation.graphIds(graphIds);
        when(federatedStore.getGraphs(testUser, graphIds, federatedOperation)).thenReturn(asList(graph1, graph3));
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(asList(graph1, graph2, graph3, graph4));

        // When
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> new FederatedOperationHandler<Void, Iterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore))
                .withMessageContaining(String.format(FederatedOperationHandler.ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS_FORMAT, ""))
                .withStackTraceContaining(errorMessage);
    }

    @Test
    public void shouldNotThrowStoreExceptionSkipFlagSetTrue() throws Exception {
        // Given
        String errorMessage = "test exception";
        FederatedStore mockStore = Mockito.mock(FederatedStore.class);
        given(mockStore.getSchema()).willReturn(new Schema());
        given(mockStore.getProperties()).willReturn(new FederatedStoreProperties());
        given(mockStore.execute(any(), any())).willThrow(new RuntimeException(errorMessage));
        final HashMap mockMap = mock(HashMap.class);
        given(mockMap.get(any())).willReturn(getDefaultMergeFunction());
        given(mockMap.getOrDefault(any(), any())).willReturn(getDefaultMergeFunction());
        given(mockStore.getStoreConfiguredDefaultMergeFunctions()).willReturn(mockMap);
        graph3 = getGraphWithMockStore(mockStore);

        FederatedStore federatedStore = mock(FederatedStore.class);

        FederatedOperation federatedOperation = getFederatedOperation(getPayload());
        federatedOperation.skipFailedFederatedExecution(true);
        final List<String> graphIds = asList("1", "2", "3");
        federatedOperation.graphIds(graphIds);
        when(federatedStore.getGraphs(testUser, getCleanStrings("1,2,3"), federatedOperation)).thenReturn(asList(graph1, graph2, graph3));
        when(federatedStore.getGraphs(testUser, graphIds, federatedOperation)).thenReturn(asList(graph1, graph2, graph3));
        when(federatedStore.getGraphs(testUser, null, federatedOperation)).thenReturn(asList(graph1, graph2, graph3, graph4));
        when(federatedStore.getGraphs(testUser, getCleanStrings(null), federatedOperation)).thenReturn(asList(graph1, graph2, graph3, graph4));
        when(federatedStore.getStoreConfiguredDefaultMergeFunctions()).thenReturn(mockMap);

        // When
        Object results = null;

        try {
            results = new FederatedOperationHandler<Void, Iterable<? extends Element>>().doOperation(federatedOperation, context, federatedStore);
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
        List<Graph> linkedGraphs = asList(graph1, graph2);

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
    public void shouldReturnEmptyOutputOfTypeIterableWhenResultsIsNull() throws Exception {
        // Given
        Output<Iterable<? extends Element>> payload = getPayload();

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore = getMockStore(unusedSchema, storeProperties);
        given(mockStore.execute(any(OperationChain.class), any(Context.class))).willReturn(null);

        FederatedStore federatedStore = Mockito.mock(FederatedStore.class);
        List<Graph> filteredGraphs = singletonList(getGraphWithMockStore(mockStore));
        given(federatedStore.getGraphs(eq(testUser), eq((List) null), any(FederatedOperation.class))).willReturn(filteredGraphs);

        // When
        final Object results = new FederatedOperationHandler().doOperation(getFederatedOperation(payload), context, federatedStore);

        // Then
        assertThat(results)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.iterable(Object.class))
                .containsExactly();
    }

    @Test
    public void shouldProcessAIterableOfBooleanFromMultipleGraphs() throws Exception {
        // Given
        Output<Iterable<? extends Element>> payload = getPayload();

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore = getMockStore(unusedSchema, storeProperties);
        given(mockStore.execute(any(OperationChain.class), any(Context.class))).willReturn(singletonList(true));

        FederatedStore federatedStore = Mockito.mock(FederatedStore.class);
        List<Graph> threeGraphsOfBoolean = asList(getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore));
        given(federatedStore.getGraphs(eq(testUser), eq((List) null), any(FederatedOperation.class))).willReturn(threeGraphsOfBoolean);

        // When
        final Object results = new FederatedOperationHandler().doOperation(getFederatedOperation(payload), context, federatedStore);

        // Then
        assertThat(results)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.iterable(Object.class))
                .containsExactly(true, true, true);
    }

    @Test
    public void shouldProcessABooleanNotJustIterablesFromMultipleGraphs() throws Exception {
        // Given
        Output<Iterable<? extends Element>> payload = getPayload();

        Store mockStore = getMockStore(new Schema(), new StoreProperties());
        given(mockStore.execute(any(OperationChain.class), any(Context.class))).willReturn(true);

        FederatedStore federatedStore = Mockito.mock(FederatedStore.class);
        List<Graph> threeGraphsOfBoolean = asList(getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore));
        given(federatedStore.getGraphs(eq(testUser), eq((List) null), any(FederatedOperation.class))).willReturn(threeGraphsOfBoolean);

        // When
        final Object results = new FederatedOperationHandler().doOperation(getFederatedOperation(payload), context, federatedStore);

        // Then
        assertThat(results)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.iterable(Object.class))
                .containsExactly(true, true, true);
    }

    @Test
    public void shouldProcessAIterableOfIntegersFromMultipleGraphs() throws Exception {
        // Given
        Output<Iterable<? extends Element>> payload = getPayload();

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore = getMockStore(unusedSchema, storeProperties);
        given(mockStore.execute(any(OperationChain.class), any(Context.class))).willReturn(singletonList(123));

        FederatedStore federatedStore = Mockito.mock(FederatedStore.class);
        List<Graph> threeGraphsOfBoolean = asList(getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore));
        given(federatedStore.getGraphs(eq(testUser), eq((List) null), any(FederatedOperation.class))).willReturn(threeGraphsOfBoolean);

        // When
        final Object results = new FederatedOperationHandler().doOperation(getFederatedOperation(payload), context, federatedStore);

        // Then
        assertThat(results)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.iterable(Object.class))
                .containsExactly(123, 123, 123);
    }

    @Test
    public void shouldProcessAIterableOfNullFromMultipleGraphs() throws Exception {
        // Given
        Output<Iterable<? extends Element>> payload = getPayload();

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore = getMockStore(unusedSchema, storeProperties);
        given(mockStore.execute(any(OperationChain.class), any(Context.class))).willReturn(singletonList((Object) null));

        FederatedStore federatedStore = Mockito.mock(FederatedStore.class);
        List<Graph> threeGraphsOfNull = asList(getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore));
        given(federatedStore.getGraphs(eq(testUser), eq((List) null), any(FederatedOperation.class))).willReturn(threeGraphsOfNull);

        // When
        final Object results = new FederatedOperationHandler().doOperation(getFederatedOperation(payload), context, federatedStore);

        // Then
        assertThat(results)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.iterable(Object.class))
                .containsExactly(null, null, null);
    }


    @Test
    public void shouldReturnNulledOutputOfTypeIterableWhenResultsContainsOnlyNull() throws Exception {
        // Given
        Output<Iterable<? extends Element>> payload = getPayload();

        Schema unusedSchema = new Schema.Builder().build();
        StoreProperties storeProperties = new StoreProperties();

        Store mockStore = getMockStore(unusedSchema, storeProperties);
        given(mockStore.execute(any(OperationChain.class), any(Context.class))).willReturn(null);

        FederatedStore federatedStore = Mockito.mock(FederatedStore.class);
        List<Graph> threeGraphsOfNull = asList(getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore), getGraphWithMockStore(mockStore));
        given(federatedStore.getGraphs(eq(testUser), eq((List) null), any(FederatedOperation.class))).willReturn(threeGraphsOfNull);

        // When
        final Object results = new FederatedOperationHandler().doOperation(getFederatedOperation(payload), context, federatedStore);

        // Then
        assertThat(results)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.iterable(Object.class))
                .containsExactly();
    }


    protected void validateMergeResultsFromFieldObjects(final Object result, final Iterable<? extends Element>... resultParts) {

        final Iterable[] resultPartItrs = Arrays.copyOf(resultParts, resultParts.length, Iterable[].class);

        final ArrayList<Element> elements = Lists.newArrayList(new ChainedIterable<>(resultPartItrs));


        final IterableAssert<Element> elementIterableAssert = assertThat(result)
                .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
                .containsExactlyInAnyOrder(elements.<Element>toArray(new Element[0]));
    }

    @Test
    public void shouldMergeVariousReturnsFromGraphs() throws Exception {
        // Given
        final BiFunction function = getDefaultMergeFunction();

        List<Integer> graph1Results = null; //null results
        List<Integer> graph2ResultsVeryNormal = asList(1, 2, 3); //normal results
        List<Integer> graph3Results = Collections.emptyList(); //empty results
        List<Integer> graph4Results = singletonList((Integer) null); // results is null
        List<Integer> graph5Results = asList(4, null, 5); //results with null
        final Iterable<Iterable<Integer>> input = asList(
                graph1Results,
                graph2ResultsVeryNormal,
                graph3Results,
                graph4Results,
                graph5Results);

        // When
        Object results = null;
        for (final Iterable<Integer> integers : input) {
            results = function.apply(integers, results);
        }

        // Then
        assertThat(function).isEqualTo(getDefaultMergeFunction());
        assertThat(results).isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.iterable(Object.class))
                .containsExactlyInAnyOrder(1, 2, 3, null, 4, null, 5);
    }

}
