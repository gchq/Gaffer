/*
 * Copyright 2022 Crown Copyright
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

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.ListAssert;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.ExecutorService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.user.StoreUser.ALL_USERS;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;
import static uk.gov.gchq.gaffer.user.User.UNKNOWN_USER_ID;

public final class FederatedStoreTestUtil {

    public static final String STRING = "String";
    public static final String ENTITIES = "Entities";
    public static final String EDGES = "Edges";
    public static final String GRAPH_ID_TEST_FEDERATED_STORE = "testFederatedStoreGraphID";
    public static final String GRAPH_ID_ACCUMULO = "AccumuloStore";
    public static final String GRAPH_ID_MAP = "MapStore";
    public static final String ACCUMULO_STORE_SINGLE_USE_PROPERTIES = "properties/singleUseAccumuloStore.properties";
    public static final String MAP_STORE_SINGLE_USE_PROPERTIES = "properties/singleUseMapStore.properties";
    public static final String GRAPH_ID_ACCUMULO_WITH_EDGES = GRAPH_ID_ACCUMULO + EDGES;
    public static final String GRAPH_ID_ACCUMULO_WITH_ENTITIES = GRAPH_ID_ACCUMULO + ENTITIES;

    public static final String GRAPH_ID_A = "graphA";
    public static final String GRAPH_ID_B = "graphB";
    public static final String FEDERATED_STORE_SINGLE_USE_PROPERTIES = "properties/singleUseFederatedStore.properties";
    public static final String SCHEMA_EDGE_BASIC_JSON = "/schema/basicEdgeSchema.json";
    public static final String SCHEMA_ENTITY_BASIC_JSON = "/schema/basicEntitySchema.json";
    public static final String SCHEMA_ENTITY_A_JSON = "/schema/entityASchema.json";
    public static final String SCHEMA_ENTITY_B_JSON = "/schema/entityBSchema.json";
    public static final String GROUP_BASIC_EDGE = "BasicEdge";
    public static final String GROUP_BASIC_ENTITY = "BasicEntity";
    public static final String BASIC_VERTEX = "basicVertex";
    public static final String FORMAT_PROPERTY_STRING = "property%s";
    public static final String PROPERTY_1 = property(1);
    public static final String PROPERTY_2 = property(2);
    public static final int VALUE_PROPERTY1 = 1;
    public static final String SOURCE_BASIC = "basicSource";
    public static final String DEST_BASIC = "basicDest";
    public static final String ACCUMULO_STORE_SINGLE_USE_PROPERTIES_ALT = "properties/singleUseAccumuloStoreAlt.properties";
    public static final String FORMAT_VALUE_STRING = "value%s";
    public static final String VALUE_1 = value(1);
    public static final String VALUE_2 = value(2);
    static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    static final Set<String> GRAPH_AUTHS_ALL_USERS = ImmutableSet.of(ALL_USERS);
    public static final String INTEGER = "integer";

    private FederatedStoreTestUtil() {
        //no instance
    }

    public static String property(final int i) {
        return String.format(FORMAT_PROPERTY_STRING, i);
    }

    public static String value(final int i) {
        return String.format(FORMAT_VALUE_STRING, i);
    }

    public static void resetForFederatedTests() {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
        ExecutorService.shutdown();
        SingleUseFederatedStore.cleanUp();
    }

    public static StoreProperties loadStoreProperties(final String path) {
        return StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreTestUtil.class, path));
    }

    public static FederatedStoreProperties loadFederatedStoreProperties(final String path) {
        return FederatedStoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreTestUtil.class, path));
    }

    public static AccumuloProperties loadAccumuloStoreProperties(final String path) {
        return AccumuloProperties.loadStoreProperties(path);
    }

    public static void addGraph(final FederatedStore federatedStore, final String graphId, final boolean isPublic, final Schema schema) throws StorageException {
        federatedStore.addGraphs(null, UNKNOWN_USER_ID, isPublic,
                new GraphSerialisable.Builder()
                        .config(new GraphConfig(graphId))
                        .schema(schema)
                        .properties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .build());
    }

    public static  Context contextBlankUser() {
        return new Context(blankUser());
    }

    public static Context contextAuthUser() {
        return new Context(authUser());
    }

    public static Schema loadSchemaFromJson(final String... path) {
        return Schema.fromJson(Arrays.stream(path)
                .map(FederatedStoreTestUtil.class::getResourceAsStream)
                .toArray(InputStream[]::new));
    }

    public static FederatedStore loadFederatedStoreFrom(final String path) throws IOException {
        return JSONSerialiser.deserialise(IOUtils.toByteArray(StreamUtil.openStream(FederatedStoreTestUtil.class, path)), FederatedStore.class);
    }

    public static Entity entityBasic() {
        return new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex(BASIC_VERTEX)
                .property(PROPERTY_1, VALUE_PROPERTY1)
                .build();
    }

    public static SchemaEntityDefinition entityBasicDefinition() {
        return new SchemaEntityDefinition.Builder()
                .vertex(STRING)
                .property(PROPERTY_1, INTEGER)
                .build();
    }

    public static SchemaEdgeDefinition edgeDefinition() {
        return new SchemaEdgeDefinition.Builder()
                .source(STRING)
                .destination(DEST_BASIC)
                .property(PROPERTY_1, STRING)
                .build();
    }

    public static Edge edgeBasic() {
        return new Edge.Builder()
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .group(GROUP_BASIC_EDGE)
                .property(PROPERTY_1, VALUE_PROPERTY1)
                .build();
    }

    public static Context contextTestUser() {
        return new Context(testUser());
    }

    public static ListAssert<GraphSerialisable> assertThatGraphs(final Collection<Graph> graphs, final GraphSerialisable... values) {
        return assertThat(graphs.stream().map(g -> new GraphSerialisable.Builder().graph(g).build()).collect(Collectors.toList())).containsExactlyInAnyOrder(values);
    }
}
