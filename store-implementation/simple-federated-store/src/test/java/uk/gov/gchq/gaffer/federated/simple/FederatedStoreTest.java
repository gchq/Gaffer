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

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.federated.simple.util.FederatedTestUtils;
import uk.gov.gchq.gaffer.federated.simple.util.FederatedTestUtils.StoreType;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;

import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_DEFAULT_GRAPH_IDS;

class FederatedStoreTest {

    @AfterEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldInitialiseNewStore() throws StoreException {
        String graphId = "federated";
        FederatedStoreProperties properties = new FederatedStoreProperties();
        FederatedStore store = new FederatedStore();
        store.initialise(graphId, null, properties);

        assertThat(store.getGraphId()).isEqualTo(graphId);
        assertThat(store.getProperties()).isEqualTo(properties);
    }

    @Test
    void shouldNotSetDefaultGraphIdsIfGraphDoesNotExist() throws StoreException {
        final String graphId = "federated";
        final String graphId1 = "graph1";
        final String graphId2 = "graph2";
        FederatedStoreProperties properties = new FederatedStoreProperties();
        // Set the defaults to graphs not available to the store
        properties.set(PROP_DEFAULT_GRAPH_IDS, graphId1 + "," + graphId2);

        FederatedStore store = new FederatedStore();
        store.initialise(graphId, null, properties);

        assertThat(store.getDefaultGraphIds()).isEmpty();
    }

    @Test
    void shouldSetDefaultGraphIdsIfGraphsExist() throws StoreException {
        final String graphId = "federated";
        final String graphId1 = "graph1";
        final String graphId2 = "graph2";
        final FederatedStore store = new FederatedStore();
        final FederatedStoreProperties properties = new FederatedStoreProperties();

        // Set the defaults to graphs and make sure to add them
        properties.set(PROP_DEFAULT_GRAPH_IDS, graphId1 + "," + graphId2);
        store.initialise(graphId, null, properties);
        store.addGraph(new GraphSerialisable(new GraphConfig(graphId1), new Schema(), new StoreProperties()),
                new GraphAccess());
        store.addGraph(new GraphSerialisable(new GraphConfig(graphId2), new Schema(), new StoreProperties()),
                new GraphAccess());

        assertThat(store.getDefaultGraphIds()).containsExactly(graphId1, graphId2);
    }

    @Test
    void shouldAddAndGetGraphsViaStoreInterface() throws StoreException, CacheOperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId1 = "graph1";
        final String graphId2 = "graph2";

        final Graph graph1 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.MAP);
        final Graph graph2 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId2, StoreType.MAP);

        final GraphSerialisable graph1Serialisable = new GraphSerialisable(graph1.getConfig(), graph1.getSchema(), graph1.getStoreProperties());
        final GraphSerialisable graph2Serialisable = new GraphSerialisable(graph2.getConfig(), graph2.getSchema(), graph2.getStoreProperties());

        final Graph expectedGraph1 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.MAP);
        final Graph expectedGraph2 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId2, StoreType.MAP);

        // When
        FederatedStore store = new FederatedStore();
        store.initialise(federatedGraphId, null, new StoreProperties());
        store.addGraph(graph1Serialisable, new GraphAccess());
        store.addGraph(graph2Serialisable, new GraphAccess());

        // Then
        assertThat(store.getGraph(graphId1)).isEqualTo(
            new GraphSerialisable(
                expectedGraph1.getConfig(),
                expectedGraph1.getSchema(),
                expectedGraph1.getStoreProperties()));
        assertThat(store.getGraph(graphId2)).isEqualTo(
            new GraphSerialisable(
                expectedGraph2.getConfig(),
                expectedGraph2.getSchema(),
                expectedGraph2.getStoreProperties()));
        assertThat(store.getAllGraphsAndAccess())
            .extracting(Pair::getLeft)
            .containsExactlyInAnyOrder(
                new GraphSerialisable(
                    expectedGraph1.getConfig(),
                    expectedGraph1.getSchema(),
                    expectedGraph1.getStoreProperties()),
            new GraphSerialisable(
                expectedGraph2.getConfig(),
                expectedGraph2.getSchema(),
                expectedGraph2.getStoreProperties()));
    }
}
