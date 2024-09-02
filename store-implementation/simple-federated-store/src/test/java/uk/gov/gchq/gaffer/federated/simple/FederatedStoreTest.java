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

import uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils;
import uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils.StoreType;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class FederatedStoreTest {

    @Test
    void shouldInitialiseNewStore() throws StoreException {
        String graphId = "federated";
        StoreProperties properties = new StoreProperties();
        FederatedStore store = new FederatedStore();
        store.initialise(graphId, null, properties);

        assertThat(store.getGraphId()).isEqualTo(graphId);
        assertThat(store.getProperties()).isEqualTo(properties);
    }

    @Test
    void shouldNotInitialiseWithSchema() {
        String graphId = "federated";
        StoreProperties properties = new StoreProperties();
        FederatedStore store = new FederatedStore();
        Schema schema = new Schema();

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> store.initialise(graphId, schema, properties));
    }

    @Test
    void shouldAddGraphsViaStoreInterface() throws StoreException {
        // Given
        final String graphId1 = "graph1";
        final String graphId2 = "graph2";

        final Graph graph1 = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.MAP);
        final Graph graph2 = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId2, StoreType.MAP);

        final GraphSerialisable graph1Serialisable = new GraphSerialisable(graph1.getConfig(), graph1.getSchema(), graph1.getStoreProperties());
        final GraphSerialisable graph2Serialisable = new GraphSerialisable(graph2.getConfig(), graph2.getSchema(), graph2.getStoreProperties());

        // When
        FederatedStore store = new FederatedStore();
        store.initialise("federated", null, new StoreProperties());
        store.addGraph(graph1Serialisable);
        store.addGraph(graph2Serialisable);

        // Then
        assertThat(store.getGraph(graphId1)).isEqualTo(graph1Serialisable);
        assertThat(store.getGraph(graphId2)).isEqualTo(graph2Serialisable);
    }
}
