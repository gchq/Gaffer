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
import org.junit.jupiter.api.Disabled;
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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.from;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.*;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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

    @Test
    public void shouldReturnSelectedGraphIdByDefaultFromJsonConfig()
            throws Exception {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromJson();
        final Schema selectedSchema = addSelectedGraph(federatedStore);

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
    public void shouldReturnSelectedGraphIdByDefaultFromJsonConfigWhenAnotherGraphIdAdded()
            throws Exception {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromJson();
        final Schema selectedSchema = addSelectedGraph(federatedStore);
        addUnselectedSchema(federatedStore);

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
    public void shouldFailGetAllGraphInfoIfGraphIdAndSchemaNoAddedFromJsonConfig()
            throws Exception {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromJson();

        // Then
        assertThat(federatedStore)
                .isNotNull()
                .returns(Lists.newArrayList(GRAPH_ID_SELECTED),
                        from(FederatedStore::getStoreConfiguredGraphIds));

        assertThat(federatedStore.getGraphs(testUser(), null, new GetAllGraphInfo()))
                .isEmpty();
    }

    @Test
    public void shouldReturnSelectedGraphIdSchemaIfSelectedGraphIdSet()
            throws OperationException {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromProperties();
        final Schema selectedSchema = addSelectedGraph(federatedStore);
        addUnselectedSchema(federatedStore);

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
    public void shouldReturnUnselectedGraphIdSchemaIfUnselectedGraphIdSet()
            throws OperationException {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromProperties();
        addSelectedGraph(federatedStore);
        final Schema unselectedSchema = addUnselectedSchema(federatedStore);

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
    public void shouldReturnSelectedGraphIdSchemaOnlyIfGraphIdsNotSet()
            throws OperationException {
        // Given
        final FederatedStore federatedStore = getFederatedStoreFromProperties();
        final Schema selectedSchema = addSelectedGraph(federatedStore);
        addUnselectedSchema(federatedStore);

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

    private static FederatedStore getFederatedStoreFromJson() throws IOException, StoreException {
        final FederatedStore federatedStore = loadFederatedStoreFrom("configuredProperties/federatedStoreConfiguredGraphIds.json");
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.setAdminAuth("hello");
        federatedStore.initialise(GRAPH_ID_SELECTED, new Schema(), properties);
        addDefaultLibrary(federatedStore);
        return federatedStore;
    }

    private static FederatedStore getFederatedStoreFromProperties() {
        final FederatedStore federatedStore = (FederatedStore) Store.createStore(GRAPH_ID_TEST_FEDERATED_STORE,
                STRING_SCHEMA,
                StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStore.class,
                        "configuredProperties/federatedStoreConfiguredGraphIds.properties")));

        addDefaultLibrary(federatedStore);
        return federatedStore;
    }

    private static void addDefaultLibrary(final FederatedStore federatedStore) {
        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.addProperties(STORE_PROP_ID, PROPERTIES);
        federatedStore.setGraphLibrary(library);
    }

    private static Schema addSelectedGraph(final FederatedStore federatedStore)
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

    private static Schema addUnselectedSchema(final FederatedStore federatedStore)
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
