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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
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
import uk.gov.gchq.gaffer.store.operation.handler.function.AggregateHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadFederatedStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getHardCodedDefaultMergeFunction;

@ExtendWith(MockitoExtension.class)
public class FederatedAggregateHandlerTest {

    private static final AccumuloProperties PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldDelegateToHandler(@Mock final FederatedStore store,
                                        @Mock final AggregateHandler handler,
                                        @Mock final Aggregate op,
                                        @Mock final Context context,
                                        @Mock final Iterable<Element> expectedResult,
                                        @Mock final Schema schema)
            throws OperationException {
        // Given
        given(store.getSchema(context)).willReturn(schema);
        given(handler.doOperation(op, schema)).willReturn((Iterable) expectedResult);

        final FederatedAggregateHandler federatedHandler = new FederatedAggregateHandler(handler);

        // When
        final Object result = federatedHandler.doOperation(op, context, store);

        // Then
        assertThat(result).isSameAs(expectedResult);
        verify(handler).doOperation(op, schema);
    }

    @Test
    public void shouldAggregateDuplicatesFromDiffStores() throws Exception {
        final FederatedStoreProperties federatedStoreProperties = loadFederatedStoreProperties("predefinedFederatedStore.properties");
        final Graph fed = new Graph.Builder()
                .config(new GraphConfig("fed"))
                .addSchema(new Schema())
                .storeProperties(federatedStoreProperties)
                .build();

        final String graphNameA = "a";
        final String graphNameB = "b";

        final Context context = new Context(new User());
        fed.execute(new OperationChain.Builder()
                .first(new AddGraph.Builder()
                        .graphId(graphNameA)
                        .schema(new Schema.Builder()
                                .edge("edge", new SchemaEdgeDefinition.Builder()
                                        .source(STRING)
                                        .destination(STRING)
                                        .build())
                                .type(STRING, String.class)
                                .build())
                        .storeProperties(PROPERTIES.clone())
                        .build())
                .then(new AddGraph.Builder()
                        .graphId(graphNameB)
                        .schema(new Schema.Builder()
                                .edge("edge", new SchemaEdgeDefinition.Builder()
                                        .source(STRING)
                                        .destination(STRING)
                                        .build())
                                .type(STRING, String.class)
                                .build())
                        .storeProperties(PROPERTIES.clone())
                        .build())
                .build(), context);

        fed.execute(getFederatedOperation(new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("edge")
                        .source("s1")
                        .dest("d1")
                        .build())
                .build())
                .graphIdsCSV(graphNameA)
                .mergeFunction(getHardCodedDefaultMergeFunction()), context);

        fed.execute(getFederatedOperation(
                new AddElements.Builder()
                        .input(new Edge.Builder()
                                .group("edge")
                                .source("s1")
                                .dest("d1")
                                .build())
                        .build())
                .graphIdsCSV(graphNameB)
                .mergeFunction(getHardCodedDefaultMergeFunction()), context);

        final Iterable<? extends Element> getAll = fed.execute(new GetAllElements(), context);

        final List<Element> list = new ArrayList<>();
        getAll.forEach(list::add);

        assertThat(list).hasSize(2);

        final Iterable<? extends Element> getAggregate = fed.execute(new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Aggregate())
                .build(), context);

        list.clear();
        getAggregate.forEach(list::add);

        assertThat(list).hasSize(1);
    }
}
