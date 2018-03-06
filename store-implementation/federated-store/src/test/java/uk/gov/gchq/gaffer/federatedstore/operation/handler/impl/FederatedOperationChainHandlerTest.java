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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.PredefinedFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.integration.FederatedStoreITs;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedOperationChainHandlerTest {

    public static final String GRAPH_IDS = PredefinedFederatedStore.MAP_GRAPH + "," + PredefinedFederatedStore.ACCUMULO_GRAPH;
    private Element[] elements = new Element[]{
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

    private Element[] elements2 = new Element[]{
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

    @Before
    @After
    public void after() {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldHandleChainWithoutSpecialFederation() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final OperationChain<Iterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                // Ensure the elements are returned form the graphs in the right order
                                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                                .build())
                        .then(new Limit<>(1))
                        .build();

        // When
        final Iterable result = store.execute(opChain, context);

        // Then - the result will contain just 1 element from the first graph
        ElementUtil.assertElementEquals(Collections.singletonList(elements[0]), result);
    }

    @Test
    public void shouldHandleChainWithIterableOutput() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final FederatedOperationChain<Element> opChain = new FederatedOperationChain.Builder<Element>()
                .operationChain(
                        new OperationChain.Builder()
                                .first(new GetAllElements())
                                .then(new Limit<>(1))
                                .build())
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                .build();

        // When
        final Iterable result = store.execute(opChain, context);

        // Then - the result will contain 2 elements - 1 from each graph
        ElementUtil.assertElementEquals(Arrays.asList(elements[0], elements[1]), result);
    }

    @Test
    public void shouldHandleChainWithNoOutput() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();


        final FederatedOperationChain<Void> opChain = new FederatedOperationChain.Builder<Void>()
                .operationChain(
                        new OperationChain.Builder()
                                .first(new AddElements.Builder()
                                        .input(elements2)
                                        .build())
                                .build())
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                .build();
        // When
        final Iterable result = store.execute(opChain, context);

        // Then
        assertNull(result);
        final CloseableIterable<? extends Element> allElements = store.execute(new GetAllElements(), context);
        ElementUtil.assertElementEquals(
                Arrays.asList(elements[0], elements[1], elements2[0], elements2[1]), allElements);
    }

    @Test
    public void shouldHandleChainWithLongOutput() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();


        final FederatedOperationChain<Long> opChain = new FederatedOperationChain.Builder<Long>()
                .operationChain(
                        new OperationChain.Builder()
                                .first(new GetAllElements())
                                .then(new Count<>())
                                .build())
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                .build();
        // When
        final Iterable result = store.execute(opChain, context);

        // Then
        assertEquals(Lists.newArrayList(1L, 1L), Lists.newArrayList(result));
    }

    @Test
    public void shouldHandleChainNestedInsideAnOperationChain() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final OperationChain<CloseableIterable<Element>> opChain = new OperationChain.Builder()
                .first(new FederatedOperationChain.Builder<Element>()
                        .operationChain(new OperationChain.Builder()
                                .first(new GetAllElements())
                                .then(new Limit<>(1))
                                .build())
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .build();

        // When
        final Iterable result = store.execute(opChain, context);

        // Then - the result will contain 2 elements - 1 from each graph
        ElementUtil.assertElementEquals(Arrays.asList(elements[0], elements[1]), result);
    }

    @Test
    public void shouldHandleChainWithExtraLimit() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new FederatedOperationChain.Builder<Element>()
                        .operationChain(new OperationChain.Builder()
                                .first(new GetAllElements())
                                .then(new Limit<>(1))
                                .build())
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .then(new Limit<>(1))
                .build();

        // When
        final Iterable result = store.execute(opChain, context);

        // Then - the result will contain 1 element from the first graph
        ElementUtil.assertElementEquals(Collections.singletonList(elements[0]), result);
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
        final FederatedStore store = (FederatedStore) Store.createStore("federatedGraph", schema, StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, "predefinedFederatedStore.properties")));

        final Context context = new Context();

        store.execute(new AddElements.Builder()
                .input(elements)
                .build(), context);

        return store;
    }
}