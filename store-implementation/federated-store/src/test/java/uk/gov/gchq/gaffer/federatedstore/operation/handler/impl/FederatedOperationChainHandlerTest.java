/*
 * Copyright 2016-2024 Crown Copyright
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadFederatedStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getDefaultMergeFunction;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

public class FederatedOperationChainHandlerTest {

    public static final String GRAPH_IDS = String.format("%s,%s", GRAPH_ID_ACCUMULO_WITH_ENTITIES, GRAPH_ID_ACCUMULO_WITH_EDGES);

    private final Element[] elements = new Element[]{
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .build()
    };

    private final Element[] elements2 = new Element[]{
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("2")
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("2")
                    .dest("3")
                    .directed(true)
                    .build()
    };

    @BeforeEach
    @AfterEach
    public void after() {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldHandleChainWithoutSpecialFederation() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit(1))
                .build();

        // When
        final Iterable result = store.execute(opChain, context);

        // Then - the result will contain just 1 element from the graphs
        assertThat(result)
                .hasSize(1)
                .containsAnyOf(elements[0], elements[1]);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldHandleChainWithIterableOutput() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final FederatedOperation opChain = getFederatedOperation(
                new OperationChain.Builder()
                        .first(new GetAllElements())
                        .then(new Limit<>(1))
                        .build())
                .graphIdsCSV(GRAPH_IDS);

        // When
        final Iterable result = (Iterable) store.execute(opChain, context);

        // Then - the result will contain 2 elements - 1 from each graph
        assertThat(result)
                .containsExactlyInAnyOrder(elements[0], elements[1]);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldHandleChainWithNoOutput() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();


        final FederatedOperation opChain = getFederatedOperation(
                new OperationChain.Builder()
                        .first(new AddElements.Builder()
                                .input(elements2)
                                .build())
                        .build())
                .mergeFunction(getDefaultMergeFunction())
                .graphIdsCSV(GRAPH_IDS);

        // When
        final Iterable result = (Iterable) store.execute(opChain, context);

        // Then
        final Iterable<Element> allElements = (Iterable<Element>) store.execute(new GetAllElements(), context);

        assertThat(allElements)
                .containsExactlyInAnyOrder(elements[0], elements[1], elements2[0], elements2[1]);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldHandleChainWithLongOutput() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final FederatedOperation opChain = getFederatedOperation(
                new OperationChain.Builder()
                        .first(new GetAllElements())
                        .then(new Count<>())
                        .build())
                .mergeFunction(new Sum())
                .graphIdsCSV(GRAPH_IDS);
        // When
        final Object result = store.execute(opChain, context);

        // Then
        assertThat(result)
                .isEqualTo(2L);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldHandleChainNestedInsideAnOperationChain() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final FederatedOperation opChain = getFederatedOperation(
                new OperationChain.Builder()
                        .first(new GetAllElements())
                        .then(new Limit<>(1))
                        .build()).graphIdsCSV(GRAPH_IDS);

        // When
        final Iterable result = (Iterable) store.execute(opChain, context);

        // Then - the result will contain 2 elements - 1 from each graph
        ElementUtil.assertElementEquals(Arrays.asList(elements[0], elements[1]), result);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldHandleChainWithExtraLimit() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        OperationChain.OutputBuilder<Iterable<? extends Element>> first = new OperationChain.Builder()
                .first(getFederatedOperation(new OperationChain.Builder()
                        .first(new GetAllElements())
                        .then(new Limit<>(1))
                        .build())
                        .graphIdsCSV(GRAPH_IDS));

        final OperationChain<Iterable<? extends Element>> opChain = first
                .then(new Limit<>(1))
                .build();

        // When
        final Iterable result = store.execute(opChain, context);

        // Then - the result will contain 1 element from the first graph
        ElementUtil.assertElementEquals(Collections.singletonList(elements[1]), result);
    }

    private FederatedStore createStore() throws OperationException {
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .aggregate(false)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .aggregate(false)
                        .build())
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .validateFunctions(new IsTrue())
                        .build())
                .build();
        final FederatedStore store = (FederatedStore) Store.createStore(GRAPH_ID_TEST_FEDERATED_STORE, schema, loadFederatedStoreProperties("predefinedFederatedStore.properties"));

        final Context context = new Context();

        store.execute(new AddElements.Builder()
                .input(elements)
                .build(), context);

        return store;
    }

}
