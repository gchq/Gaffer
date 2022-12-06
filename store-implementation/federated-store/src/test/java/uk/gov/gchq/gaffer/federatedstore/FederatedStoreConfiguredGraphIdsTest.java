/*
 * Copyright 2021-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.from;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadFederatedStoreFrom;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadStoreProperties;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreConfiguredGraphIdsTest {

    private static final Schema STRING_SCHEMA = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();

    private static final StoreProperties PROPERTIES = loadStoreProperties("properties/singleUseMapStore.properties");
    private static final String STORE_PROP_ID = "storeProp";
    private static final String GRAPH_ID_SELECTED = "selectedGraphId";
    private static final String SCHEMA_SELECTED = "selectedSchema";
    private static final String GRAPH_ID_UNSELECTED = "unselectedGraphId";
    private static final String SCHEMA_UNSELECTED = "unselectedSchema";
    public static final String FEDERATED_STORE_CONFIGURED_GRAPH_IDS_PROPERTIES = "configuredProperties/federatedStoreConfiguredGraphIds.properties";
    public static final String FEDERATED_STORE_CONFIGURED_GRAPH_IDS_JSON = "configuredProperties/federatedStoreConfiguredGraphIds.json";
    public static final String FEDERATED_STORE_CONFIGURED_GRAPH_IDS_FAIL_PATH_MISSING_PROPERTIES = "configuredProperties/federatedStoreConfiguredGraphIdsFailPathMissing.properties";
    public static final String FEDERATED_STORE_CONFIGURED_GRAPH_IDS_FAIL_FILE_INVALID_PROPERTIES = "configuredProperties/federatedStoreConfiguredGraphIdsFailFileInvalid.properties";

    @Test
    void shouldFailConfiguredGraphIdsPathMissing() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getFederatedStoreFromProperties(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_FAIL_PATH_MISSING_PROPERTIES))
                .withMessageContaining("Failed to create input stream for path: fail");
    }

    @Test
    void shouldFailConfiguredGraphIdsFileInvalid() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getFederatedStoreFromProperties(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_FAIL_FILE_INVALID_PROPERTIES))
                .withMessageContaining("Could not initialise the store with provided arguments.")
                .withStackTraceContaining("Cannot deserialize value of type `java.util.ArrayList<java.lang.Object>` from String value");
    }

    @Test
    void shouldReturnSelectedGraphIdByDefaultFromJsonConfig()
            throws Exception {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromJson(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_JSON);
        final Schema selectedSchema = addSelectedSchemaAndGraph(federatedStore);

        // Then
        assertThat(federatedStore)
                .isNotNull()
                .returns(Lists.newArrayList(GRAPH_ID_SELECTED),
                        from(FederatedStore::getStoreConfiguredGraphIds));

        final List<GraphSerialisable> graphs = federatedStore.getGraphs(testUser(),
                null,
                new GetAllGraphInfo());

        assertThat(graphs).hasSize(1);
        assertThat(graphs.get(0).getGraphId()).isEqualTo(GRAPH_ID_SELECTED);
        assertThat(graphs.get(0).getSchema()).isEqualTo(selectedSchema);
    }

    @Test
    void shouldReturnSelectedGraphIdByDefaultFromJsonConfigWhenAnotherGraphIdAdded()
            throws Exception {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromJson(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_JSON);
        final Schema selectedSchema = addSelectedSchemaAndGraph(federatedStore);
        addUnselectedSchemaAndGraph(federatedStore);

        // Then
        assertThat(federatedStore)
                .isNotNull()
                .returns(Lists.newArrayList(GRAPH_ID_SELECTED),
                        from(FederatedStore::getStoreConfiguredGraphIds));

        final List<GraphSerialisable> graphs = federatedStore.getGraphs(testUser(),
                null,
                new GetAllGraphInfo());

        assertThat(graphs).hasSize(1);
        assertThat(graphs.get(0).getGraphId()).isEqualTo(GRAPH_ID_SELECTED);
        assertThat(graphs.get(0).getSchema()).isEqualTo(selectedSchema);
    }

    @Test
    void shouldFailGetAllGraphInfoIfGraphIdAndSchemaNoAddedFromJsonConfig()
            throws Exception {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromJson(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_JSON);

        // Then
        assertThat(federatedStore)
                .isNotNull()
                .returns(Lists.newArrayList(GRAPH_ID_SELECTED),
                        from(FederatedStore::getStoreConfiguredGraphIds));

        assertThat(federatedStore.getGraphs(testUser(), null, new GetAllGraphInfo()))
                .isEmpty();
    }

    @Test
    void shouldReturnSelectedGraphIdSchemaIfSelectedGraphIdSet()
            throws OperationException {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromProperties(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_PROPERTIES);
        final Schema selectedSchema = addSelectedSchemaAndGraph(federatedStore);
        addUnselectedSchemaAndGraph(federatedStore);

        // When
        final FederatedOperation<Object, Object> federatedOperation = new FederatedOperation.Builder()
                .op(new GetSchema.Builder()
                        .compact(true)
                        .build())
                .graphIds(Collections.singletonList(GRAPH_ID_SELECTED))
                .build();
        final Schema result = (Schema) federatedStore.execute(federatedOperation, contextTestUser());

        // Then
        assertNotNull(result);
        JsonAssert.assertEquals(selectedSchema.toJson(true), result.toJson(true));
    }

    @Test
    void shouldReturnUnselectedGraphIdSchemaIfUnselectedGraphIdSet()
            throws OperationException {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromProperties(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_PROPERTIES);
        addSelectedSchemaAndGraph(federatedStore);
        final Schema unselectedSchema = addUnselectedSchemaAndGraph(federatedStore);

        // When
        final FederatedOperation<Object, Object> federatedOperation = new FederatedOperation.Builder()
                .op(new GetSchema.Builder()
                        .compact(true)
                        .build())
                .graphIds(Collections.singletonList(GRAPH_ID_UNSELECTED))
                .build();
        final Schema result = (Schema) federatedStore.execute(federatedOperation, contextTestUser());

        // Then
        assertNotNull(result);
        JsonAssert.assertEquals(unselectedSchema.toJson(true), result.toJson(true));
    }

    @Test
    void shouldReturnSelectedGraphIdSchemaOnlyIfGraphIdsNotSet()
            throws OperationException {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromProperties(FEDERATED_STORE_CONFIGURED_GRAPH_IDS_PROPERTIES);
        final Schema selectedSchema = addSelectedSchemaAndGraph(federatedStore);
        addUnselectedSchemaAndGraph(federatedStore);

        // When
        final FederatedOperation<Object, Object> federatedOperation = new FederatedOperation.Builder()
                .op(new GetSchema.Builder()
                        .compact(true)
                        .build())
                .build();
        final Schema result = (Schema) federatedStore.execute(federatedOperation, contextTestUser());

        // Then
        assertNotNull(result); // TODO: Fails as result == null
        JsonAssert.assertEquals(selectedSchema.toJson(true), result.toJson(true));
    }

    private static FederatedStore getFederatedStoreFromJson(final String path) throws IOException, StoreException {
        final FederatedStore federatedStore = loadFederatedStoreFrom(path);
        federatedStore.initialise(GRAPH_ID_SELECTED, new Schema(), new FederatedStoreProperties());
        addDefaultLibrary(federatedStore);
        return federatedStore;
    }

    private static FederatedStore getFederatedStoreFromProperties(final String path) {
        final FederatedStore federatedStore = (FederatedStore) Store.createStore(GRAPH_ID_TEST_FEDERATED_STORE,
                STRING_SCHEMA,
                StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStore.class, path)));

        addDefaultLibrary(federatedStore);
        return federatedStore;
    }

    private static void addDefaultLibrary(final FederatedStore federatedStore) {
        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.addProperties(STORE_PROP_ID, PROPERTIES);
        federatedStore.setGraphLibrary(library);
    }

    private static Schema addSelectedSchemaAndGraph(final FederatedStore federatedStore)
            throws OperationException {
        final Schema selectedSchema = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .property(PROPERTY_1, STRING)
                        .directed(DIRECTED_EITHER)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        federatedStore.getGraphLibrary().addSchema(SCHEMA_SELECTED, selectedSchema);
        addGraph(federatedStore, GRAPH_ID_SELECTED, SCHEMA_SELECTED, contextTestUser());

        return selectedSchema;
    }

    private static Schema addUnselectedSchemaAndGraph(final FederatedStore federatedStore)
            throws OperationException {
        final Schema unselectedSchema = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .property(PROPERTY_2, STRING)
                        .directed(DIRECTED_EITHER)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        federatedStore.getGraphLibrary().addSchema(SCHEMA_UNSELECTED, unselectedSchema);
        addGraph(federatedStore, GRAPH_ID_UNSELECTED, SCHEMA_UNSELECTED, contextTestUser());

        return unselectedSchema;
    }

    private static void addGraph(final FederatedStore federatedStore, final String graphId,
                                 final String schemaId, final Context context)
            throws OperationException {
        federatedStore.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId(graphId)
                        .parentPropertiesId(STORE_PROP_ID)
                        .parentSchemaIds(singletonList(schemaId))
                        .build()), context);
    }
}
