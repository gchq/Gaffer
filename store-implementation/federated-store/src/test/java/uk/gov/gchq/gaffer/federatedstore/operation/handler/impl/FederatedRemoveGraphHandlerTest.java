/*
 * Copyright 2017-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.basicEntitySchema;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedRemoveGraphHandlerTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private User testUser;

    private static Class currentClass = new Object() {
    }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));

    @BeforeEach
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        testUser = testUser();
    }

    @Test
    public void shouldRemoveGraphForOwningUser() throws Exception {
        FederatedStore store = new FederatedStore();
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

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
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

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
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

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
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

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
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        final String removeThisCache = "removeThisCache";
        final String myViewToRemove = "myViewToRemove";

        assertThat(CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache)))
                .isNotNull()
                .returns(null, c -> c.get(myViewToRemove))
                .returns(0, ICache::size);

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

        assertThat(CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache)))
                .isNotNull()
                .returns(1, ICache::size)
                .doesNotReturn(null, cache -> cache.get(myViewToRemove));

        assertEquals(1, store.getGraphs(testUser, null, new RemoveGraph()).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .removeCache(false)
                        .build(),
                new Context(testUser),
                store);

        assertThat(CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache)))
                .isNotNull()
                .returns(1, ICache::size)
                .doesNotReturn(null, c -> c.get(myViewToRemove));

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).isEmpty();
    }

    @Test
    public void shouldRemoveGraphAndCache() throws Exception {
        FederatedStore store = new FederatedStore();
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        final String removeThisCache = "removeThisCache";
        final String myViewToRemove = "myViewToRemove";

        assertThat(CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache)))
                .isNotNull()
                .returns(null, c -> c.get(myViewToRemove))
                .returns(0, ICache::size);

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

        assertThat(CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache)))
                .isNotNull()
                .returns(1, ICache::size)
                .doesNotReturn(null, cache -> cache.get(myViewToRemove));

        assertEquals(1, store.getGraphs(testUser, null, new RemoveGraph()).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        //.removeCache(true)  User default value
                        .build(),
                new Context(testUser),
                store);

        assertThat(CacheServiceLoader.getDefaultService().getCache(NamedViewCache.getCacheNameFrom(removeThisCache)))
                .isNotNull()
                .returns(0, ICache::size)
                .returns(null, c -> c.get(myViewToRemove));

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new RemoveGraph());

        assertThat(graphs).isEmpty();
    }
}
