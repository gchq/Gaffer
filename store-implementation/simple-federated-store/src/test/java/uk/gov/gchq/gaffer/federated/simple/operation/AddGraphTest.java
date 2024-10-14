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

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class AddGraphTest {

    @AfterEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldAddGraphUsingBuilder() throws StoreException, OperationException, CacheOperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId = "newGraph";

        // Create the expected graph that should've been added
        final GraphSerialisable expectedSerialisable = new GraphSerialisable.Builder()
                .config(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new Properties())
                .build();

        // Build operation
        final AddGraph operation = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new Properties())
                .build();

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        federatedStore.execute(operation, new Context());
        final GraphSerialisable addedGraph = federatedStore.getGraph(graphId);

        // Then
        assertThat(addedGraph.getConfig().getGraphId())
            .isEqualTo(expectedSerialisable.getConfig().getGraphId());
        assertThat(addedGraph.getSchema())
            .isEqualTo(expectedSerialisable.getSchema());
        assertThat(addedGraph.getStoreProperties().getProperties())
            .isEqualTo(expectedSerialisable.getStoreProperties().getProperties());
    }

    @Test
    void shouldAddGraphUsingJSONSerialisation()
            throws StoreException, OperationException, SerialisationException, CacheOperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId = "newGraph";
        final StoreProperties storeProperties = new MapStoreProperties();
        storeProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.mapstore.MapStore");

        // Set up the graph we expect to be added
        final GraphSerialisable expectedSerialisable = new GraphSerialisable(
            new GraphConfig(graphId), null, storeProperties);

        // JSON version of the operation
        final JSONObject jsonOperation = new JSONObject()
            .put("class", "uk.gov.gchq.gaffer.federated.simple.operation.AddGraph")
            .put("graphConfig", new JSONObject()
                .put("graphId", graphId))
            .put("properties", new JSONObject()
                .put("gaffer.store.class", "uk.gov.gchq.gaffer.mapstore.MapStore")
                .put("gaffer.store.properties.class", "uk.gov.gchq.gaffer.mapstore.MapStoreProperties"));

        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // When
        final AddGraph operation = JSONSerialiser.deserialise(jsonOperation.toString(), AddGraph.class);
        federatedStore.execute(operation, new Context());

        final GraphSerialisable addedGraph = federatedStore.getGraph(graphId);

        // Then
        assertThat(addedGraph.getGraphId()).isEqualTo(expectedSerialisable.getGraphId());
        assertThat(addedGraph.getSchema()).isEqualTo(expectedSerialisable.getSchema());
        assertThat(addedGraph.getStoreProperties().getProperties())
            .isEqualTo(expectedSerialisable.getStoreProperties().getProperties());
    }

    @Test
    void shouldPreventAddingGraphWithoutGraphConfig() throws StoreException {
        // Given
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise("federated", null, new StoreProperties());
        final Context context = new Context();

        final AddGraph operation = new AddGraph.Builder()
            .schema(new Schema())
            .properties(new Properties())
            .build();

        // When/Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() ->federatedStore.execute(operation, context))
            .withMessageContaining("graphConfig is required");
    }

    @Test
    void shouldPreventAddingGraphsWithSameIds() throws StoreException, OperationException {
        // Given
        final String graphId = "graph";
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise("federated", null, new StoreProperties());
        final Context context = new Context();

        AddGraph operation = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new Properties())
                .build();

        // Add same graph twice expect fail second time
        federatedStore.execute(operation, context);

        // Then
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> federatedStore.execute(operation, context))
                .withMessageContaining("already been added to this store");
    }

    @Test
    void shouldAddGraphWithAccessUsingJSONSerialisation()
            throws StoreException, OperationException, SerialisationException, CacheOperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId = "newGraph";
        final StoreProperties storeProperties = new MapStoreProperties();
        storeProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.mapstore.MapStore");

        // JSON version of the operation
        final JSONObject jsonOperation = new JSONObject()
                .put("class", "uk.gov.gchq.gaffer.federated.simple.operation.AddGraph")
                .put("graphConfig", new JSONObject()
                        .put("graphId", graphId))
                .put("properties", new JSONObject()
                        .put("gaffer.store.class", "uk.gov.gchq.gaffer.mapstore.MapStore")
                        .put("gaffer.store.properties.class", "uk.gov.gchq.gaffer.mapstore.MapStoreProperties"))
                .put("owner", User.UNKNOWN_USER_ID)
                .put("isPublic", true)
                .put("readPredicate", new JSONObject()
                    .put("class", "uk.gov.gchq.gaffer.access.predicate.AccessPredicate")
                    .put("userPredicate", new JSONObject()
                        .put("class", "uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate")
                        .put("auths", new JSONArray()
                            .put("test"))));

        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // When
        final AddGraph operation = JSONSerialiser.deserialise(jsonOperation.toString(), AddGraph.class);
        System.out.print(operation.getReadPredicate());
        federatedStore.execute(operation, new Context());

        final GraphAccess addedAccess = federatedStore.getGraphAccess(graphId);

        // Then
        // Check the access that was added
        assertThat(addedAccess.getOwner()).isEqualTo(User.UNKNOWN_USER_ID);
        assertThat(addedAccess.isPublic()).isTrue();
        assertThat(addedAccess.getResourceType()).isEqualTo(ResourceType.FederatedStoreGraph);
        assertThat(addedAccess.getReadAccessPredicate())
            .isExactlyInstanceOf(AccessPredicate.class);
        assertThat(addedAccess.getReadAccessPredicate().getUserPredicate())
            .isExactlyInstanceOf(DefaultUserPredicate.class);
        assertThat(addedAccess.getWriteAccessPredicate())
            .isExactlyInstanceOf(UnrestrictedAccessPredicate.class);
    }

    @Test
    void shouldPreventAddingPublicGraphsToStoresThatDoNotAllowIt() throws StoreException {
        final String graphId = "graph";
        final String federatedGraphId = "federated";
        final FederatedStore federatedStore = new FederatedStore();
        FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(FederatedStoreProperties.PROP_ALLOW_PUBLIC_GRAPHS, "false");

        federatedStore.initialise(federatedGraphId, null, properties);

        // Build operation
        final AddGraph operation = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new Properties())
                .isPublic(true)
                .build();

        assertThatExceptionOfType(OperationException.class)
            .isThrownBy(() -> federatedStore.execute(operation, new Context()))
            .withMessageContaining("Public graphs are not allowed");
    }

}
