/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils.StoreType;

class FederatedStoreIT {

    @Test
    void shouldFederateElementsByAggregation() throws StoreException, OperationException {
        // Given
        FederatedStore store = new FederatedStore();

        final String graphId1 = "graph1";
        final String graphId2 = "graph2";

        final Graph graph1 = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.MAP);
        final Graph graph2 = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId2, StoreType.MAP);

        final String group = "person";
        final String vertex = "1";

        // Add the same vertex to different graphs but with different properties
        Properties graph1ElementProps = new Properties();
        graph1ElementProps.put("name", "marko");
        Entity graph1Entity = new Entity(group, vertex, graph1ElementProps);

        Properties graph2ElementProps = new Properties();
        graph2ElementProps.put("age", 29);
        Entity graph2Entity = new Entity(group, vertex, graph2ElementProps);

        OperationChain<Void> addGraph1Elements = new OperationChain.Builder()
            .first(new AddElements.Builder()
                .input(graph1Entity)
                .build())
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1)
            .build();

        OperationChain<Void> addGraph2Elements = new OperationChain.Builder()
            .first(new AddElements.Builder()
                    .input(graph2Entity)
                    .build())
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId2)
            .build();

        // We expect only one entity to be returned that has merged properties
        Properties mergedProperties = new Properties();
        mergedProperties.putAll(graph1ElementProps);
        mergedProperties.putAll(graph2ElementProps);
        Entity expectedEntity = new Entity(group, vertex, mergedProperties);

        // Init store and add graphs
        store.initialise("federated", null, new StoreProperties());
        store.addGraph(new GraphSerialisable(
                graph1.getConfig(),
                graph1.getSchema(),
                graph1.getStoreProperties()));
        store.addGraph(new GraphSerialisable(
                graph2.getConfig(),
                graph2.getSchema(),
                graph2.getStoreProperties()));

        // Add data into graphs
        store.execute(addGraph1Elements, new Context());
        store.execute(addGraph2Elements, new Context());

        // Run a get all on both graphs specifying that we want to merge elements
        OperationChain<Iterable<? extends Element>> getAllElements = new OperationChain.Builder()
            .first(new GetAllElements.Builder()
                    .build())
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1 + "," + graphId2)
            .option(FederatedOperationHandler.OPT_AGGREGATE_ELEMENTS, "true")
            .build();

        Iterable<? extends Element> result = store.execute(getAllElements, new Context());

        // Then
        assertThat(result).extracting(e -> (Element) e).containsOnly(expectedEntity);
    }

}
