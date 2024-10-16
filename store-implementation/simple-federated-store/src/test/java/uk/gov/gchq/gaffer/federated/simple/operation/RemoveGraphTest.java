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

package uk.gov.gchq.gaffer.federated.simple.operation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.util.FederatedTestUtils;
import uk.gov.gchq.gaffer.federated.simple.util.FederatedTestUtils.StoreType;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class RemoveGraphTest {

    @AfterEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldRemoveGraphAndPreserveData() throws StoreException, OperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId = "shouldRemoveGraphAndPreserveData";
        final Graph originalGraph = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId, StoreType.ACCUMULO);

        // Entity so we can check data still exists
        final Properties graphEntityProps = new Properties();
        graphEntityProps.put("name", "marko");
        final Entity graphEntity = new Entity("person", "1", graphEntityProps);

        // Remove graph operation
        final RemoveGraph removeGraph = new RemoveGraph.Builder()
            .graphId(graphId)
            .build();

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // Add the graph and its elements then remove it
        FederatedTestUtils.addGraphWithElements(federatedStore, originalGraph, graphEntity);
        federatedStore.execute(removeGraph, new Context());

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> federatedStore.getGraph(graphId))
            .withMessageContaining("not available to this federated store");

        // See if the graph  data still exists in the cluster
        Iterable<? extends Element> result = originalGraph.execute(new GetAllElements(), new Context());
        assertThat(result).extracting(e -> (Element) e).containsExactly(graphEntity);
    }

    @Test
    void shouldRemoveGraphAndDeleteAllData() throws StoreException, OperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId = "shouldRemoveGraphAndDeleteAllData";
        final Graph originalGraph = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId, StoreType.ACCUMULO);

        // Entity so we can check data still exists
        final Properties graphEntityProps = new Properties();
        graphEntityProps.put("name", "marko");
        final Entity graphEntity = new Entity("person", "1", graphEntityProps);

        // Remove graph operation with delete all option set
        final RemoveGraph removeGraph = new RemoveGraph.Builder()
                .graphId(graphId)
                .deleteAllData(true)
                .build();

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // Add the graph and its elements then remove it
        FederatedTestUtils.addGraphWithElements(federatedStore, originalGraph, graphEntity);
        federatedStore.execute(removeGraph, new Context());

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> federatedStore.getGraph(graphId))
                .withMessageContaining("not available to this federated store");

        // See if the graph data still exists in the cluster
        Iterable<? extends Element> result = originalGraph.execute(new GetAllElements(), new Context());
        assertThat(result).isEmpty();
    }

    @Test
    void shouldNotRemoveGraphThatDoesntExist() throws StoreException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId = "shouldNotRemoveGraphThatDoesntExist";
        final RemoveGraph removeGraph = new RemoveGraph.Builder()
                .graphId(graphId)
                .build();
        final Context context = new Context();

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> federatedStore.execute(removeGraph, context))
                .withMessageContaining("not available to this federated store");
    }

    @Test
    void shouldNotRemoveAccessControlledGraph() throws StoreException, OperationException {
        final String federatedGraphId = "federated";
        final String graphId = "shouldNotRemoveAccessControlledGraph";

        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // Add a graph that no one can edit
        final AddGraph addGraph = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new StoreProperties().getProperties())
                .writePredicate(new NoAccessPredicate())
                .build();

        // Remove operation
        final RemoveGraph removeGraph = new RemoveGraph.Builder()
                .graphId(graphId)
                .build();

        // When
        federatedStore.execute(addGraph, new Context());

        // Then
        assertThatExceptionOfType(OperationException.class)
            .isThrownBy(() -> federatedStore.execute(removeGraph, new Context()))
            .withMessageContaining("does not have write permissions");
    }
}
