/*
 * Copyright 2016-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedAggregateHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.function.AggregateHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FederatedAggregateHandlerTest {
    @Test
    public void shouldDelegateToHandler() throws OperationException {
        // Given
        final FederatedStore store = mock(FederatedStore.class);
        final AggregateHandler handler = mock(AggregateHandler.class);
        final Aggregate op = mock(Aggregate.class);
        final Context context = mock(Context.class);
        final Iterable expectedResult = mock(Iterable.class);
        final Schema schema = mock(Schema.class);

        given(store.getSchema(op, context)).willReturn(schema);
        given(handler.doOperation(op, schema)).willReturn(expectedResult);

        final FederatedAggregateHandler federatedHandler = new FederatedAggregateHandler(handler);

        // When
        final Object result = federatedHandler.doOperation(op, context, store);

        // Then
        assertSame(expectedResult, result);
        verify(handler).doOperation(op, schema);
    }

    @Test
    public void shouldAggregateDuplicatesFromDiffStores() throws Exception {
        final Graph fed = new Graph.Builder()
                .config(new GraphConfig("fed"))
                .addSchema(new Schema())
                .storeProperties(new FederatedStoreProperties())
                .build();

        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setStoreClass(MockAccumuloStore.class);

        final Context context = new Context(new User());
        fed.execute(new OperationChain.Builder()
                .first(new AddGraph.Builder()
                        .graphId("a")
                        .schema(new Schema.Builder()
                                .edge("edge", new SchemaEdgeDefinition.Builder()
                                        .source("string")
                                        .destination("string")
                                        .build())
                                .type("string", String.class)
                                .build())
                        .storeProperties(storeProperties)
                        .build())
                .then(new AddGraph.Builder()
                        .graphId("b")
                        .schema(new Schema.Builder()
                                .edge("edge", new SchemaEdgeDefinition.Builder()
                                        .source("string")
                                        .destination("string")
                                        .build())
                                .type("string", String.class)
                                .build())
                        .storeProperties(storeProperties)
                        .build())
                .build(), context);

        fed.execute(new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("edge")
                        .source("s1")
                        .dest("d1")
                        .build())
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "a")
                .build(), context);

        fed.execute(new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("edge")
                        .source("s1")
                        .dest("d1")
                        .build())
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "b")
                .build(), context);

        final CloseableIterable<? extends Element> getAll = fed.execute(new GetAllElements(), context);

        List<Element> list = new ArrayList<>();
        getAll.forEach(list::add);

        assertEquals(2, list.size());

        final Iterable<? extends Element> getAggregate = fed.execute(new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Aggregate())
                .build(), context);

        list.clear();
        getAggregate.forEach(list::add);

        assertEquals(1, list.size());
    }
}
