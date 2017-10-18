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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.PredefinedFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.integration.FederatedStoreITs;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedOperationChainHandlerTest {
    public static final String GRAPH_IDS = PredefinedFederatedStore.ALL_GRAPH_IDS;
    public static final String OTHER_GRAPH_IDS = "graph1,graph2,graph3";

    @Test
    public void shouldDelegateToDefaultOperationChainHandlerWhenTheChainNeedsSplitting() throws OperationException {
        // Given
        final FederatedStore store = mock(FederatedStore.class);
        final Context context = mock(Context.class);
        final Iterable expectedResult = mock(Iterable.class);
        final OperationChain<Iterable<? extends String>> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>())
                .then(new DiscardOutput())
                .then(new GetAllGraphIds())
                .build();

        final OperationChainHandler<Iterable<? extends String>> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Iterable<? extends String>> federatedHandler = new FederatedOperationChainHandler<>(handler);

        given(handler.doOperation(opChain, context, store)).willReturn(expectedResult);

        // When
        final Iterable<? extends String> result = federatedHandler.doOperation(opChain, context, store);

        // Then
        assertSame(expectedResult, result);
        verify(handler).doOperation(opChain, context, store);
    }

    @Test
    public void shouldHandleTheEntireChain() throws OperationException {
        // Given
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
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>(10))
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                .build();

        final OperationChainHandler<Iterable<? extends Element>> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Iterable<? extends Element>> federatedHandler = new FederatedOperationChainHandler<>(handler);

        final Element[] elements = {
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1")
                        .dest("2")
                        .directed(true)
                        .build()
        };

        federatedHandler.doOperation(
                new OperationChain<>(
                        new AddElements.Builder()
                                .input(elements)
                                .build()
                ), context, store);

        // When
        final Iterable result = federatedHandler.doOperation(opChain, context, store);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(elements), result);
        verify(handler).prepareOperationChain(opChain, context, store);
        verify(handler, never()).doOperation(opChain, context, store);
    }

    @Test
    public void shouldHandleTheEntireChainWhenNoGraphIdsProvided() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldHandleTheEntireChainWhenGraphIdsJustProvidedOnOpChain() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>())
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldHandleTheEntireChainWhenGraphIdsJustProvidedOnInnerOperations() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .then(new Limit.Builder<>()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldHandleTheEntireChainWhenGraphIdsProvidedOnBothOpChainAndInnerOperations() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .then(new Limit.Builder<>()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldNotHandleTheEntireChainWhenGraphIdsAreInconsistent() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .then(new Limit.Builder<>()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, OTHER_GRAPH_IDS)
                        .build())
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldNotHandleTheEntireChainWhenGraphIdsAreInconsistent2() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .then(new Limit.Builder<>()
                        .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, GRAPH_IDS)
                        .build())
                .option(KEY_OPERATION_OPTIONS_GRAPH_IDS, OTHER_GRAPH_IDS)
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldHandleTheEntireChainWhenOutputTypeIsVoid() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>())
                .then(new DiscardOutput())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldHandleTheEntireChainWhenOutputTypeIsAnIterable() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldHandleTheEntireChainWhenOutputTypeIsACloseableIterable() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldNotHandleTheEntireChainWhenOutputTypeIsAnElement() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Max())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldNotHandleTheEntireChainWhenOutputTypeIsALong() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Count<>())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldNotHandleTheEntireChainWhenItIncludesFederatedOperations() throws OperationException {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Count<>())
                .then(new DiscardOutput())
                .then(new AddGraph())
                .build();

        final OperationChainHandler<Object> handler = mock(OperationChainHandler.class);
        final FederatedOperationChainHandler<Object> federatedHandler = new FederatedOperationChainHandler<>(handler);

        // When
        final boolean result = federatedHandler.canHandleEntireChain(opChain);

        // Then
        assertFalse(result);
    }
}