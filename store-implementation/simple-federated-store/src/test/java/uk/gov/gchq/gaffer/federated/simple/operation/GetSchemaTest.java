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

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.BooleanSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.BytesSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;

class GetSchemaTest {

    @AfterEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldMergeSchemasWithConflictingVisibilities() throws StoreException, OperationException {
        // Define schemas with different visibility properties
        Schema schema1 = new Schema.Builder()
            .visibilityProperty("schema1").build();

        Schema schema2 = new Schema.Builder()
                .visibilityProperty("schema2").build();

        String graphId1 = "graph1";
        String graphId2 = "graph2";

        FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise("federated", null, new FederatedStoreProperties());

        federatedStore.addGraph(
            new GraphSerialisable(new GraphConfig(graphId1), schema1, new MapStoreProperties()),
            new GraphAccess());
        federatedStore.addGraph(
            new GraphSerialisable(new GraphConfig(graphId2), schema2, new MapStoreProperties()),
            new GraphAccess());

        GetSchema getOp = new GetSchema.Builder()
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1 + "," + graphId2)
            .build();

        Schema result = federatedStore.execute(getOp, new Context());

        assertThat(result).isNotNull();
        assertThat(result.getVisibilityProperty()).isNull();
    }

    @Test
    void shouldMergeSchemasWithConflictingSerialisers() throws StoreException, OperationException {
        // Define schemas with different visibility properties
        Schema schema1 = new Schema.Builder()
                .vertexSerialiser(new BytesSerialiser()).build();

        Schema schema2 = new Schema.Builder()
                .vertexSerialiser(new BooleanSerialiser()).build();

        String graphId1 = "graph1";
        String graphId2 = "graph2";

        FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise("federated", null, new FederatedStoreProperties());

        federatedStore.addGraph(
                new GraphSerialisable(new GraphConfig(graphId1), schema1, new MapStoreProperties()),
                new GraphAccess());
        federatedStore.addGraph(
                new GraphSerialisable(new GraphConfig(graphId2), schema2, new MapStoreProperties()),
                new GraphAccess());

        GetSchema getOp = new GetSchema.Builder()
                .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1 + "," + graphId2)
                .build();

        Schema result = federatedStore.execute(getOp, new Context());

        assertThat(result).isNotNull();
        assertThat(result.getVertexSerialiser()).isNull();
    }

}
