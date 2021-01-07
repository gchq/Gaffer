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

package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FederatedViewValidationTest {
    private static final AccumuloProperties ACCUMULO_PROPERTIES = AccumuloProperties.loadStoreProperties(
            StreamUtil.openStream(new Object() {
            }.getClass().getEnclosingClass(), "properties/singleUseAccumuloStore.properties"));

    /**
     * There is no Federated ViewValidation any more,
     * but incorrect view should not cause errors.
     *
     * @throws OperationException exception
     */
    @Test
    public void shouldNotErrorWithInvalidView() throws OperationException {
        //Given
        final Graph graph = new Graph.Builder()
                .addStoreProperties(new FederatedStoreProperties())
                .config(new GraphConfig.Builder().graphId("testFedGraph").build())
                .build();

        String edgeGraph = "edgeGraph";
        String type = "type";
        String edgeGroup = "edgeGroup";
        graph.execute(new AddGraph.Builder()
                .graphId(edgeGraph)
                .isPublic(true)
                .storeProperties(ACCUMULO_PROPERTIES)
                .schema(new Schema.Builder()
                        .edge(edgeGroup, new SchemaEdgeDefinition.Builder()
                                .source(type)
                                .destination(type)
                                .build())
                        .type(type, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .build())
                        .build())
                .build(), new Context());

        Edge testEdge = new Edge.Builder()
                .group(edgeGroup)
                .source("A")
                .dest("B")
                .build();
        graph.execute(new AddElements.Builder()
                .input(testEdge)
                .build(), new Context());

        //When
        String missingEntity = "missingEntity";
        CloseableIterable<? extends Element> missingEntityIt = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(missingEntity)
                        .build())
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, edgeGraph)
                .build(), new Context());

        CloseableIterable<? extends Element> edgeIt = graph.execute(new GetAllElements.Builder()
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, edgeGraph)
                .build(), new Context());

        CloseableIterable<? extends Element> edgePlusIt = graph.execute(new GetAllElements.Builder()
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, edgeGraph)
                .view(new View.Builder()
                        .edges(Lists.newArrayList(edgeGroup, "extraEdgeGroup"))
                        .entity(missingEntity)
                        .build())
                .build(), new Context());

        //Then
        assertFalse(missingEntityIt.iterator().hasNext());
        assertTrue(edgeIt.iterator().hasNext());
        CloseableIterator<? extends Element> edgePlusIterator = edgePlusIt.iterator();
        assertTrue(edgePlusIterator.hasNext());

        assertEquals(testEdge, edgePlusIterator.next());
        assertFalse(edgePlusIterator.hasNext());

    }
}
