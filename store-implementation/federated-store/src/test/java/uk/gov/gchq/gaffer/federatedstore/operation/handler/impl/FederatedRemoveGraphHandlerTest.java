/*
 * Copyright 2017-2024 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreVisibilityTest;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.*;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedRemoveGraphHandlerTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private User testUser;
    private Graph federatedGraph;

    private static final AccumuloProperties PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    public static FederatedStoreProperties createProperties() {
        FederatedStoreProperties fedProps  = new FederatedStoreProperties();
        try {
            Properties props = new Properties();
            props.load(FederatedStoreVisibilityTest.class.getResourceAsStream("/properties/federatedStore.properties"));
            fedProps.setProperties(props);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fedProps .setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        return fedProps;
    }

    @BeforeEach
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        FederatedStoreProperties props = createProperties();
        testUser = testUser();

        federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(props)
                .build();
    }

    @Test
    public void shouldRemoveGraphForOwningUser() throws Exception {
        FederatedStore store = new FederatedStore();
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, getFederatedStorePropertiesWithHashMapCache());

        store.addGraphs(testUser.getOpAuths(), testUser.getUserId(), false, new GraphSerialisable.Builder()
                .config(new GraphConfig(EXPECTED_GRAPH_ID))
                .schema(new Schema.Builder().build())
                .properties(PROPERTIES)
                .build());

        assertEquals(1, store.getGraphs(testUser, null, new RemoveGraph()).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .build(),
                new Context(testUser),
                store);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).isEmpty();

    }

    @Test
    public void shouldNotRemoveGraphForNonOwningUser() throws Exception {
        FederatedStore store = new FederatedStore();
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, getFederatedStorePropertiesWithHashMapCache());

        store.addGraphs(testUser.getOpAuths(), "other", false, new GraphSerialisable.Builder()
                .config(new GraphConfig(EXPECTED_GRAPH_ID))
                .schema(new Schema.Builder().build())
                .properties(PROPERTIES)
                .build());

        assertEquals(1, store.getGraphs(testUser, null, new RemoveGraph()).size());

        final Boolean removed = new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .build(),
                new Context(testUser),
                store);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).hasSize(1);
        assertFalse(removed);
    }

    @Test
    public void shouldReturnFalseWhenNoGraphWasRemoved() throws Exception {
        FederatedStore store = new FederatedStore();
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, getFederatedStorePropertiesWithHashMapCache());

        assertEquals(0, store.getGraphs(testUser, null, new RemoveGraph()).size());

        final Boolean removed = new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .build(),
                new Context(testUser),
                store);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).hasSize(0);
        assertFalse(removed);

    }

    @Test
    public void shouldNotRemoveGraphConfiguredWithNoAccessWritePredicate() throws Exception {
        FederatedStore store = new FederatedStore();
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, getFederatedStorePropertiesWithHashMapCache());

        final AccessPredicate noAccessPredicate = new NoAccessPredicate();

        store.addGraphs(
                testUser.getOpAuths(),
                "other",
                false,
                null,
                noAccessPredicate,
                new GraphSerialisable.Builder()
                        .config(new GraphConfig(EXPECTED_GRAPH_ID))
                        .schema(new Schema.Builder().build())
                        .properties(PROPERTIES)
                        .build());

        assertEquals(1, store.getGraphs(testUser, null, new RemoveGraph()).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .build(),
                new Context(testUser),
                store);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).hasSize(1);
    }

    @Test
    public void shouldRemoveGraphButNotCache() throws Exception {
        FederatedStore store = new FederatedStore();
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, getFederatedStorePropertiesWithHashMapCache());

        final String removeThisCache = "removeThisCache";
        final String myViewToRemove = "myViewToRemove";

        final ICache<Object, Object> cache = CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache));

        assertThat(cache).isNotNull();
        assertThat(cache.get(myViewToRemove)).isNull();
        assertThat(cache.size()).isZero();

        final AccumuloProperties clone = PROPERTIES.clone();
        clone.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        clone.setCacheServiceNameSuffix(removeThisCache);

        store.execute(new AddGraph.Builder()
                .graphId(EXPECTED_GRAPH_ID)
                .schema(basicEntitySchema())
                .isPublic(true)
                .storeProperties(clone)
                .build(), contextTestUser());

        store.execute(new FederatedOperation.Builder().op(new AddNamedView.Builder().name(myViewToRemove).view(new View.Builder().edge(GROUP_BASIC_ENTITY, new ViewElementDefinition.Builder().properties(PROPERTY_1).build()).build()).build()).build(), contextTestUser());

        assertThat(cache.get(myViewToRemove)).isNotNull();
        assertThat(cache.size()).isEqualTo(1);
        assertThat(store.getGraphs(testUser, null, new RemoveGraph())).hasSize(1);

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .removeCache(false)
                        .build(),
                new Context(testUser),
                store);

        assertThat(cache.get(myViewToRemove)).isNotNull();
        assertThat(cache.size()).isEqualTo(1);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).isEmpty();
    }

    @Test
    public void shouldRemoveGraphAndCache() throws Exception {
        FederatedStore store = new FederatedStore();
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, getFederatedStorePropertiesWithHashMapCache());

        final String removeThisCache = "removeThisCache";
        final String myViewToRemove = "myViewToRemove";
        final ICache<Object, Object> cache = CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache));

        assertThat(cache).isNotNull();
        assertThat(cache.get(myViewToRemove)).isNull();
        assertThat(cache.size()).isZero();

        final AccumuloProperties clone = PROPERTIES.clone();
        clone.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        clone.setCacheServiceNameSuffix(removeThisCache);

        store.execute(new AddGraph.Builder()
                .graphId(EXPECTED_GRAPH_ID)
                .schema(basicEntitySchema())
                .isPublic(true)
                .storeProperties(clone)
                .build(), contextTestUser());

        store.execute(new FederatedOperation.Builder().op(new AddNamedView.Builder().name(myViewToRemove).view(new View.Builder().edge(GROUP_BASIC_ENTITY, new ViewElementDefinition.Builder().properties(PROPERTY_1).build()).build()).build()).build(), contextTestUser());

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(myViewToRemove)).isNotNull();
        assertThat(store.getGraphs(testUser, null, new RemoveGraph())).hasSize(1);

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        //.removeCache(true)  User default value
                        .build(),
                new Context(testUser),
                store);

        assertThat(cache.size()).isZero();
        assertThat(cache.get(myViewToRemove)).isNull();

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).isEmpty();
    }

    @Test
    public void shouldRemoveGraphAndCacheWhenUsingMultipleServices() throws Exception {
        // Create and initialise a new Federated Store with the default cache service class store property configured
        FederatedStore store = new FederatedStore();
        FederatedStoreProperties props = createProperties();
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, props);

        final String cacheNameSuffix = "removeThisCache";
        final String viewName = "myViewToRemove";

        // Get default cache (initialised by the Federated Store) and check any cache entry for the view is empty
        final ICache<Object, Object> defaultCache = CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(cacheNameSuffix));
        assertThat(defaultCache).isNotNull();
        assertThat(defaultCache.size()).isZero();

        // Set Accumulo Store properties to use separate cache instances/services for Named Views and set suffix
        final AccumuloProperties accumuloProperties = PROPERTIES.clone();
        accumuloProperties.setNamedViewCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        accumuloProperties.setCacheServiceNameSuffix(cacheNameSuffix);

        // Add graph to the Federated Store using the Accumulo Store properties
        store.execute(new AddGraph.Builder()
                .graphId(EXPECTED_GRAPH_ID)
                .schema(basicEntitySchema())
                .isPublic(true)
                .storeProperties(accumuloProperties)
                .build(), contextTestUser());

        // Get NamedView specific cache and check any cache entry for the view is empty
        final ICache<Object, Object> namedViewSpecificCache = CacheServiceLoader.getService(NamedViewCache.NAMED_VIEW_CACHE_SERVICE_NAME).getCache(NamedViewCache.getCacheNameFrom(cacheNameSuffix));
        assertThat(namedViewSpecificCache).isNotNull();
        assertThat(namedViewSpecificCache.size()).isZero();

        // Add a view to the new Accumulo backed graph
        store.execute(new FederatedOperation.Builder().op(new AddNamedView.Builder().name(viewName).view(new View.Builder().edge(GROUP_BASIC_ENTITY, new ViewElementDefinition.Builder().properties(PROPERTY_1).build()).build()).build()).build(), contextTestUser());

        // Check that cache entry for the view is present in the NamedView specific cache
        assertThat(namedViewSpecificCache.size()).isEqualTo(1);
        assertThat(namedViewSpecificCache.get(viewName)).isNotNull();

        // Check there is no cache entry for the view in the default cache
        assertThat(defaultCache).isNotNull();
        assertThat(defaultCache.size()).isZero();

        // Check there is a graph present in the Federated Store
        assertThat(store.getGraphs(testUser, null, new RemoveGraph())).hasSize(1);

        // Remove the Accumulo backed graph from the Federated Store
        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        //.removeCache(true)  User default value
                        .build(),
                new Context(testUser),
                store);

        // Check that the cache entry for the view has been removed from the NamedView specific cache
        assertThat(namedViewSpecificCache.size()).isZero();
        assertThat(namedViewSpecificCache.get(viewName)).isNull();

        // Check that graph has been removed from the Federated Store
        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());
        assertThat(graphs).isEmpty();
    }
}
