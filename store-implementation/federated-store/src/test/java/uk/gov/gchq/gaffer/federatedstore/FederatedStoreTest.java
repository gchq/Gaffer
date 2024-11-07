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

package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.exception.CloneFailedException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetTraitsHandlerTest;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.user.User;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreCacheTransient.getCacheNameFrom;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES_ALT;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.DEST_BASIC;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_AUTHS_ALL_USERS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_A_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_B_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SOURCE_BASIC;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getCleanStrings;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;
import static uk.gov.gchq.gaffer.store.StoreTrait.MATCHED_VERTEX;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.user.StoreUser.ALL_USERS;
import static uk.gov.gchq.gaffer.user.StoreUser.TEST_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreTest {
    private static final String ID_SCHEMA_ENTITY = "basicEntitySchema";
    private static final String ID_SCHEMA_EDGE = "basicEdgeSchema";
    private static final String ID_PROPS_ACC_1 = "miniAccProps1";
    private static final String ID_PROPS_ACC_2 = "miniAccProps2";
    private static final String ID_PROPS_ACC_ALT = "miniAccProps3";
    private static final String INVALID = "invalid";
    private static final String UNUSUAL_KEY = "unusualKey";
    private static final String KEY_DOES_NOT_BELONG = UNUSUAL_KEY + " was added to " + ID_PROPS_ACC_2 + " it should not be there";
    private static final String PATH_INCOMPLETE_SCHEMA = "/schema/edgeX2NoTypesSchema.json";
    private static final String PATH_INCOMPLETE_SCHEMA_PART_2 = "/schema/edgeTypeSchema.json";
    private static final String ACC_ID_1 = "miniAccGraphId1";
    private static final String ACC_ID_2 = "miniAccGraphId2";
    private static final String MAP_ID_1 = "miniMapGraphId1";
    private static final String FED_ID_1 = "subFedGraphId1";
    private static final String INVALID_CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.invalid";
    private static final String CACHE_SERVICE_NAME = getCacheNameFrom(GRAPH_ID_TEST_FEDERATED_STORE);
    private static AccumuloProperties properties1;
    private static AccumuloProperties properties2;
    private static AccumuloProperties propertiesAlt;
    private FederatedStore store;
    private FederatedStoreProperties federatedProperties;
    private HashMapGraphLibrary library;
    private Context blankUserContext;
    private User blankUser;

    @AfterAll
    public static void cleanUp() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedProperties = getFederatedStorePropertiesWithHashMapCache();
        federatedProperties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));

        properties1 = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
        properties2 = properties1.clone();
        propertiesAlt = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES_ALT);

        library = new HashMapGraphLibrary();
        library.addProperties(ID_PROPS_ACC_1, properties1);
        library.addProperties(ID_PROPS_ACC_2, properties2);
        library.addProperties(ID_PROPS_ACC_ALT, propertiesAlt);
        library.addSchema(ID_SCHEMA_EDGE, getSchemaFromPath(SCHEMA_EDGE_BASIC_JSON));
        library.addSchema(ID_SCHEMA_ENTITY, getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON));


        store = new FederatedStore();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);
        store.setGraphLibrary(library);

        blankUserContext = contextBlankUser();
        blankUser = blankUser();
    }

    @AfterEach
    public void tearDown() throws Exception {
        assertThat(properties1).withFailMessage("Library has changed: " + ID_PROPS_ACC_1).isEqualTo(library.getProperties(ID_PROPS_ACC_1));
        assertThat(properties1).withFailMessage("Library has changed: " + ID_PROPS_ACC_1).isEqualTo(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES));
        assertThat(properties2).withFailMessage("Library has changed: " + ID_PROPS_ACC_2).isEqualTo(library.getProperties(ID_PROPS_ACC_2));
        assertThat(propertiesAlt).withFailMessage("Library has changed: " + ID_PROPS_ACC_ALT).isEqualTo(library.getProperties(ID_PROPS_ACC_ALT));

        assertThat(new String(getSchemaFromPath(SCHEMA_EDGE_BASIC_JSON).toJson(false), StandardCharsets.UTF_8))
                .withFailMessage("Library has changed: " + ID_SCHEMA_EDGE)
                .isEqualTo(new String(library.getSchema(ID_SCHEMA_EDGE).toJson(false), StandardCharsets.UTF_8));
        assertThat(new String(getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON).toJson(false), StandardCharsets.UTF_8))
                .withFailMessage("Library has changed: " + ID_SCHEMA_ENTITY)
                .isEqualTo(new String(library.getSchema(ID_SCHEMA_ENTITY).toJson(false), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldLoadGraphsWithIds() throws Exception {
        //given
        final Collection<GraphSerialisable> before = store.getGraphs(blankUser, null, new GetAllGraphIds());

        //when
        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_EDGE);
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC_1, ID_SCHEMA_ENTITY);

        //then
        final Collection<GraphSerialisable> graphs = store.getGraphs(blankUser, null, new GetAllGraphIds());

        assertThat(before).size().isEqualTo(0);
        final List<String> graphNames = asList(ACC_ID_1, ACC_ID_2);
        for (final GraphSerialisable graph : graphs) {
            assertThat(graphNames).contains(graph.getGraphId());
        }
        assertThat(graphs).size().isEqualTo(2);
    }

    @Test
    public void shouldThrowErrorForFailedSchemaID() {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, INVALID));

        assertContains(actual.getCause(), SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S,
                Arrays.toString(new String[]{INVALID}));
    }

    @Test
    public void shouldThrowErrorForFailedPropertyID() {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, INVALID, ID_SCHEMA_EDGE));

        assertContains(actual.getCause(), STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, INVALID);
    }

    @Test
    public void shouldThrowErrorForMissingProperty() {
        // When / Then
        final List<String> schemas = singletonList(ID_SCHEMA_EDGE);
        final Exception actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .isPublic(true)
                        .parentSchemaIds(schemas)
                        .build(), blankUserContext));

        assertContains(actual.getCause(), GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, ACC_ID_2, "StoreProperties");
    }

    @Test
    public void shouldThrowErrorForMissingSchema() {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .isPublic(true)
                        .parentPropertiesId(ID_PROPS_ACC_2)
                        .build(), blankUserContext));

        assertContains(actual.getCause(), GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, ACC_ID_2, "Schema");
    }

    @Test
    public void shouldNotAllowOverwritingOfGraphWithinFederatedScope() throws Exception {
        // Given
        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_ENTITY);

        // When / Then
        Exception actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_EDGE));
        assertContains(actual, "User is attempting to overwrite a graph");
        assertContains(actual, "GraphId: ", ACC_ID_2);

        // When / Then
        actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_ALT, ID_SCHEMA_ENTITY));
        assertContains(actual, "User is attempting to overwrite a graph");
        assertContains(actual, "GraphId: ", ACC_ID_2);
    }

    @Test
    public void shouldThrowAppropriateExceptionWhenHandlingAnUnsupportedOperation() {
        // Given
        final Operation unknownOperation = new Operation() {
            @Override
            public Operation shallowClone() throws CloneFailedException {
                return this;
            }

            @Override
            public Map<String, String> getOptions() {
                return null;
            }

            @Override
            public void setOptions(final Map<String, String> options) {

            }
        };
        // When
        // Expected an UnsupportedOperationException rather than an OperationException

        // Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> store.handleOperation(unknownOperation, new Context()))
                .withMessageContaining("Operation class uk.gov.gchq.gaffer.federatedstore.FederatedStoreTest$1 is not supported by the FederatedStore.");
    }

    @Test
    public void shouldAlwaysReturnSupportedTraits() throws Exception {
        // Given
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC_1, ID_SCHEMA_ENTITY);

        final Set<StoreTrait> before = store.execute(new GetTraits.Builder()
                .currentTraits(false)
                .build(), blankUserContext);

        // When
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);

        final Set<StoreTrait> after = store.execute(new GetTraits.Builder()
                .currentTraits(false)
                .build(), blankUserContext);

        // Then
        assertThat(AccumuloStore.TRAITS).hasSameSizeAs(before);
        assertThat(AccumuloStore.TRAITS).hasSameSizeAs(after);
        assertThat(before).isEqualTo(after);
    }

    @Test
    @Deprecated
    public void shouldUpdateSchemaWhenNewGraphIsAdded() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);
        final Schema before = store.getSchema(new Context(blankUser), true);
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_EDGE_BASIC_JSON);
        final Schema after = store.getSchema(new Context(blankUser), true);
        // Then
        assertThat(before).isNotEqualTo(after);
    }

    @Test
    @Deprecated
    public void shouldUpdateSchemaWhenNewGraphIsRemoved() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);
        final Schema was = store.getSchema(new Context(blankUser), true);
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_EDGE_BASIC_JSON);

        final Schema before = store.getSchema(new Context(blankUser), true);

        // When
        store.remove(ACC_ID_2, blankUser, false);

        final Schema after = store.getSchema(new Context(blankUser), true);
        assertThat(before).isNotEqualTo(after);
        assertThat(was).isEqualTo(after);
    }

    @Test
    public void shouldFailWithIncompleteSchema() {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> addGraphWithPaths(ACC_ID_1, propertiesAlt, blankUserContext, PATH_INCOMPLETE_SCHEMA));
        assertContains(actual, FederatedAddGraphHandler.ERROR_ADDING_GRAPH_GRAPH_ID_S, ACC_ID_1);
    }

    @Test
    public void shouldTakeCompleteSchemaFromTwoFiles() throws Exception {
        // Given
        final int before = store.getGraphs(blankUser, null, new GetAllGraphIds()).size();
        addGraphWithPaths(ACC_ID_1, propertiesAlt, blankUserContext, PATH_INCOMPLETE_SCHEMA, PATH_INCOMPLETE_SCHEMA_PART_2);

        // When
        final int after = store.getGraphs(blankUser, null, new GetAllGraphIds()).size();

        // Then
        assertThat(before).isZero();
        assertThat(after).isEqualTo(1);
    }

    @Test
    public void shouldAddTwoGraphs() throws Exception {
        // Given
        final int sizeBefore = store.getGraphs(blankUser, null, new GetAllGraphIds()).size();

        // When
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);
        addGraphWithPaths(ACC_ID_1, propertiesAlt, blankUserContext, SCHEMA_EDGE_BASIC_JSON);

        final int sizeAfter = store.getGraphs(blankUser, null, new GetAllGraphIds()).size();

        // Then
        assertThat(sizeBefore).isZero();
        assertThat(sizeAfter).isEqualTo(2);
    }

    @Test
    public void shouldCombineTraitsToMin() throws Exception {
        //Given
        final FederatedOperation getTraits = getFederatedOperation(new GetTraits.Builder()
                .currentTraits(true)
                .build());

        //When
        final Object before = store.execute(getTraits, blankUserContext);
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        store.execute(new AddGraph.Builder()
                .schema(new Schema())
                .isPublic(true)
                .graphId(ACC_ID_1)
                .storeProperties(properties1)
                .build(), new Context(testUser()));

        final Set<StoreTrait> afterAcc = store.execute(new GetTraits.Builder()
                .currentTraits(true)
                .build(), blankUserContext);

        final StoreProperties TestStoreImp = new StoreProperties();
        TestStoreImp.setStoreClass(FederatedGetTraitsHandlerTest.TestStoreImpl.class);

        store.execute(new AddGraph.Builder()
                .schema(new Schema())
                .isPublic(true)
                .graphId(MAP_ID_1)
                .storeProperties(TestStoreImp)
                .build(), new Context(testUser()));

        final Set<StoreTrait> afterMap = store.execute(new GetTraits.Builder()
                .currentTraits(true)
                .build(), blankUserContext);

        // Then
        assertThat(SingleUseAccumuloStore.TRAITS).isNotEqualTo(new HashSet<>(asList(
                StoreTrait.INGEST_AGGREGATION,
                StoreTrait.PRE_AGGREGATION_FILTERING,
                StoreTrait.POST_AGGREGATION_FILTERING,
                StoreTrait.TRANSFORMATION,
                StoreTrait.POST_TRANSFORMATION_FILTERING,
                StoreTrait.MATCHED_VERTEX)));
        assertThat(before).withFailMessage("No traits should be found for an empty FederatedStore");
        assertThat(afterAcc).isEqualTo(new HashSet<>(Arrays.asList(
                TRANSFORMATION,
                PRE_AGGREGATION_FILTERING,
                POST_AGGREGATION_FILTERING,
                POST_TRANSFORMATION_FILTERING,
                ORDERED,
                MATCHED_VERTEX)));
        assertThat(afterMap).isEqualTo(new HashSet<>(Arrays.asList(
                TRANSFORMATION,
                PRE_AGGREGATION_FILTERING,
                POST_AGGREGATION_FILTERING,
                POST_TRANSFORMATION_FILTERING,
                MATCHED_VERTEX)));
    }

    @Test
    public void shouldContainNoElements() throws Exception {
        // When
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);
        final Set<Element> after = getElements(blankUserContext);

        // Then
        assertThat(after).isEmpty();
    }

    @Test
    public void shouldAddEdgesToOneGraph() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_EDGE_BASIC_JSON);

        final AddElements op = new AddElements.Builder()
                .input(new Edge.Builder()
                        .group(GROUP_BASIC_EDGE)
                        .source(SOURCE_BASIC)
                        .dest(DEST_BASIC)
                        .property(PROPERTY_1, 12)
                        .build())
                .build();

        // When
        store.execute(op, blankUserContext);

        // Then
        assertThat(getElements(blankUserContext)).hasSize(1);
    }

    @Test
    public void shouldReturnGraphIds() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_EDGE_BASIC_JSON);

        // When
        final Collection<String> allGraphIds = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphIds)
                .hasSize(2)
                .contains(ACC_ID_1, ACC_ID_2);

    }

    @Test
    public void shouldUpdateGraphIds() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);

        // When
        final Collection<String> allGraphId = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphId).hasSize(1)
                .contains(ACC_ID_1)
                .doesNotContain(ACC_ID_2);

        // When
        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_ENTITY);
        final Collection<String> allGraphId2 = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphId2).hasSize(2).contains(ACC_ID_1, ACC_ID_2);

        // When
        store.remove(ACC_ID_1, blankUser, false);
        final Collection<String> allGraphId3 = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphId3).hasSize(1)
                .doesNotContain(ACC_ID_1)
                .contains(ACC_ID_2);

    }

    @Test
    public void shouldGetAllGraphIdsInUnmodifiableSet() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);

        // When / Then
        final Collection<String> allGraphIds = store.getAllGraphIds(blankUser);

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> allGraphIds.add("newId"))
                .isNotNull();

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> allGraphIds.remove("newId"))
                .isNotNull();
    }

    @Test
    public void shouldNotUseSchema() throws Exception {
        // Given
        final Schema unusedMock = Mockito.mock(Schema.class);
        // When
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, unusedMock, federatedProperties);
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_EDGE_BASIC_JSON);
        // Then
        Mockito.verifyNoMoreInteractions(unusedMock);
    }

    @Test
    public void shouldAddGraphFromLibrary() throws Exception {
        // Given
        library.add(ACC_ID_2, library.getSchema(ID_SCHEMA_ENTITY), library.getProperties(ID_PROPS_ACC_2));

        // When
        final int before = store.getGraphs(blankUser, null, new GetAllGraphIds()).size();
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .build(), new Context(blankUser));

        final int after = store.getGraphs(blankUser, null, new GetAllGraphIds()).size();

        // Then
        assertThat(before).isZero();
        assertThat(after).isEqualTo(1);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibrary() throws Exception {
        // When
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .parentPropertiesId(ID_PROPS_ACC_ALT)
                .isPublic(true)
                .schema(getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON))
                .build(), blankUserContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds())).hasSize(1);
        assertThat(propertiesAlt).isEqualTo(library.getProperties(ID_PROPS_ACC_ALT));
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibrary() throws Exception {
        // When
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .storeProperties(propertiesAlt)
                .isPublic(true)
                .parentSchemaIds(singletonList(ID_SCHEMA_ENTITY))
                .build(), blankUserContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds())).hasSize(1);
        assertThat(library.getSchema(ID_SCHEMA_ENTITY).toString()).isEqualTo(getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON).toString());
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibrary() throws Exception {
        // When
        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_ALT, ID_SCHEMA_ENTITY);

        // Then
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds())).hasSize(1);
        final GraphSerialisable graph = store.getGraphs(blankUser, getCleanStrings(ACC_ID_2), new GetAllGraphIds()).iterator().next();
        assertThat(getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON)).isEqualTo(graph.getSchema());
        assertThat(graph.getStoreProperties()).isEqualTo(propertiesAlt);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibraryOverridden() throws Exception {
        // Given
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();

        // When
        final Builder schema = new Builder();
        for (final String path : new String[]{SCHEMA_ENTITY_BASIC_JSON}) {
            schema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .storeProperties(propertiesAlt)
                .parentPropertiesId(ID_PROPS_ACC_2)
                .isPublic(true)
                .schema(schema.build())
                .build(), blankUserContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds())).hasSize(1);
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds()).iterator().next().getStoreProperties().containsKey(UNUSUAL_KEY)).isTrue();
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds()).iterator().next().getStoreProperties().getProperties().getProperty(UNUSUAL_KEY)).isNotNull();
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibraryOverridden() throws Exception {
        final List<String> schemas = singletonList(ID_SCHEMA_ENTITY);
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .isPublic(true)
                .schema(getSchemaFromPath(SCHEMA_EDGE_BASIC_JSON))
                .parentSchemaIds(schemas)
                .parentPropertiesId(ID_PROPS_ACC_2)
                .build(), blankUserContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds())).hasSize(1);
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds()).iterator().next().getSchema().getEntityGroups()).contains("BasicEntity");
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibraryOverridden() throws Exception {
        // Given
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();

        // When
        final Builder tempSchema = new Builder();
        for (final String path : new String[]{SCHEMA_EDGE_BASIC_JSON}) {
            tempSchema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .isPublic(true)
                .storeProperties(propertiesAlt)
                .parentPropertiesId(ID_PROPS_ACC_2)
                .schema(tempSchema.build())
                .parentSchemaIds(singletonList(ID_SCHEMA_ENTITY))
                .build(), blankUserContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds())).hasSize(1);
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds()).iterator().next().getStoreProperties().containsKey(UNUSUAL_KEY)).isTrue();
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds()).iterator().next().getStoreProperties().getProperties().getProperty(UNUSUAL_KEY)).isNotNull();
        assertThat(store.getGraphs(blankUser, null, new GetAllGraphIds()).iterator().next().getSchema().getEntityGroups().contains("BasicEntity")).isTrue();
    }

    @Test
    public void shouldNotAllowOverridingOfKnownGraphInLibrary() {
        // Given
        library.add(ACC_ID_2, getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON), propertiesAlt);

        // When / Then
        Exception actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .parentPropertiesId(ID_PROPS_ACC_1)
                        .isPublic(true)
                        .build(), blankUserContext));
        assertContains(actual.getCause(), "Graph: " + ACC_ID_2 + " already exists so you cannot use a different StoreProperties");

        // When / Then
        actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .parentSchemaIds(singletonList(ID_SCHEMA_EDGE))
                        .isPublic(true)
                        .build(), blankUserContext));

        assertContains(actual.getCause(), "Graph: " + ACC_ID_2 + " already exists so you cannot use a different Schema");
    }

    @Test
    public void shouldFederatedIfUserHasCorrectAuths() throws Exception {
        // Given
        store.addGraphs(GRAPH_AUTHS_ALL_USERS, null, false, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(ACC_ID_2)
                        .build())
                .properties(propertiesAlt)
                .schema(getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON))
                .build());

        // When
        final Iterable<? extends Element> elements = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(blankUser.getUserId())
                        .opAuth(ALL_USERS)
                        .build()));

        // Then
        assertThat(elements.iterator()).isExhausted();

        // When - user cannot see any graphs
        final Iterable<? extends Element> elements2 = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(blankUser.getUserId())
                        .opAuths("x")
                        .build()));

        // Then
        assertThat(elements2).isEmpty();
    }

    @Test
    public void shouldReturnSpecificGraphsFromCSVString() throws Exception {
        // Given
        final List<Collection<GraphSerialisable>> graphLists = populateGraphs(1, 2, 4);
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);
        final Collection<GraphSerialisable> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<GraphSerialisable> returnedGraphs = store.getGraphs(blankUser, getCleanStrings("mockGraphId1,mockGraphId2,mockGraphId4"), new GetAllGraphIds());
        // Then
        assertThat(returnedGraphs)
                .hasSize(3)
                .containsAll(expectedGraphs);

        assertThat(returnedGraphs).doesNotContainAnyElementsOf(unexpectedGraphs);
    }

    @Test
    public void shouldReturnNotReturnEnabledOrDisabledGraphsWhenNotInCsv() throws Exception {
        // Given
        populateGraphs();

        // When
        final Collection<GraphSerialisable> returnedGraphs = store.getGraphs(blankUser, getCleanStrings("mockGraphId0,mockGraphId1"), new GetAllGraphIds());

        // Then
        final Set<String> graphIds = returnedGraphs.stream().map(GraphSerialisable::getGraphId).collect(Collectors.toSet());
        assertThat(graphIds).containsExactly("mockGraphId0", "mockGraphId1");
    }

    @Test
    public void shouldReturnNoGraphsFromEmptyString() throws Exception {
        // Given

        final List<Collection<GraphSerialisable>> graphLists = populateGraphs();
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);

        // When
        final Collection<GraphSerialisable> returnedGraphs = store.getGraphs(blankUser, getCleanStrings(""), new GetAllGraphIds());

        // Then
        assertThat(returnedGraphs).withFailMessage(returnedGraphs.toString()).isEmpty();
        assertThat(expectedGraphs).withFailMessage(expectedGraphs.toString()).isEmpty();
    }

    @Test
    public void shouldReturnGraphsWithLeadingCommaString() throws Exception {
        // Given
        final List<Collection<GraphSerialisable>> graphLists = populateGraphs(2, 4);
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);
        final Collection<GraphSerialisable> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<GraphSerialisable> returnedGraphs = store.getGraphs(blankUser, getCleanStrings(",mockGraphId2,mockGraphId4"), new GetAllGraphIds());

        // Then
        assertThat(returnedGraphs)
                .hasSize(2)
                .containsAll(expectedGraphs);

        assertThat(returnedGraphs).doesNotContainAnyElementsOf(unexpectedGraphs);
    }

    @Test
    public void shouldAddGraphIdWithAuths() throws Exception {
        // Given
        library.add(ACC_ID_1, getSchemaFromPath(SCHEMA_ENTITY_BASIC_JSON), propertiesAlt);

        // When
        Iterable<? extends String> before = store.execute(new GetAllGraphIds(), contextBlankUser());

        store.execute(new AddGraph.Builder()
                        .graphAuths("auth")
                        .graphId(ACC_ID_1)
                        .build(),
                contextBlankUser());

        Iterable<String> after = (Iterable<String>) store.execute(new GetAllGraphIds(), contextBlankUser());

        store.execute(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group("BasicEntity")
                                .vertex("v1")
                                .build())
                        .build(),
                contextBlankUser());

        final Iterable<? extends Element> elements = store.execute(
                new GetAllElements(),
                new Context(new User.Builder()
                        .userId(TEST_USER_ID + "Other")
                        .opAuth("auth")
                        .build()));

        final Iterable<? extends Element> elements2 = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(TEST_USER_ID + "Other")
                        .opAuths("x")
                        .build()));


        // Then
        assertThat(before).isEmpty();
        assertThat(after).containsExactly(ACC_ID_1);
        assertThat(elements)
                .isNotNull()
                .isNotEmpty();
        assertThat(elements2).isEmpty();
    }

    @Test
    public void shouldThrowWithPropertiesErrorFromGraphLibrary() throws Exception {
        final Builder schema = new Builder();
        for (final String path : new String[]{SCHEMA_EDGE_BASIC_JSON}) {
            schema.merge(getSchemaFromPath(path));
        }
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "test Something went wrong";
        Mockito.when(mockLibrary.getProperties(ID_PROPS_ACC_2)).thenThrow(new IllegalArgumentException(error));
        store.setGraphLibrary(mockLibrary);
        CacheServiceLoader.shutdown();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        // When / Then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .parentPropertiesId(ID_PROPS_ACC_2)
                        .isPublic(true)
                        .schema(schema.build())
                        .build(), blankUserContext))
                .withStackTraceContaining(error);
        Mockito.verify(mockLibrary).getProperties(ID_PROPS_ACC_2);
    }

    @Test
    public void shouldThrowWithSchemaErrorFromGraphLibrary() throws Exception {
        // Given
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "test Something went wrong";
        Mockito.when(mockLibrary.getSchema(ID_SCHEMA_ENTITY)).thenThrow(new IllegalArgumentException(error));
        store.setGraphLibrary(mockLibrary);
        CacheServiceLoader.shutdown();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        // When / Then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .storeProperties(propertiesAlt)
                        .isPublic(true)
                        .parentSchemaIds(singletonList(ID_SCHEMA_ENTITY))
                        .build(), blankUserContext))
                .withStackTraceContaining(error);
        Mockito.verify(mockLibrary).getSchema(ID_SCHEMA_ENTITY);
    }

    @Test
    public void shouldReturnASingleGraph() throws Exception {
        // Given
        final List<Collection<GraphSerialisable>> graphLists = populateGraphs(1);
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);
        final Collection<GraphSerialisable> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<GraphSerialisable> returnedGraphs = store.getGraphs(blankUser, getCleanStrings("mockGraphId1"), new GetAllGraphIds());

        // Then
        assertThat(returnedGraphs)
                .hasSize(1)
                .containsAll(expectedGraphs);

        assertThat(returnedGraphs).doesNotContainAnyElementsOf(unexpectedGraphs);
    }

    private List<Graph> toGraphs(final Collection<GraphSerialisable> graphSerialisables) {
        return graphSerialisables.stream().map(GraphSerialisable::getGraph).collect(Collectors.toList());
    }

    @Test
    public void shouldThrowExceptionWithInvalidCacheClass() {
        federatedProperties.setDefaultCacheServiceClass(INVALID_CACHE_SERVICE_CLASS_STRING);

        CacheServiceLoader.shutdown();

        assertThatIllegalArgumentException().isThrownBy(() -> store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties))
                .withMessageContaining("Failed to instantiate cache");
    }

    @Test
    public void shouldReuseGraphsAlreadyInCache() throws Exception {
        // Check cache is empty
        CacheServiceLoader.shutdown();
        federatedProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        assertThat(CacheServiceLoader.getDefaultService()).isNull();

        // initialise FedStore
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        // add something so it will be in the cache
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_2))
                .properties(propertiesAlt)
                .schema(StreamUtil.openStream(FederatedStoreTest.class, SCHEMA_EDGE_BASIC_JSON))
                .build();

        store.addGraphs(null, TEST_USER_ID, true, graphToAdd);

        // check the store and the cache
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
        assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME))
                .contains(ACC_ID_2, ACC_ID_2);

        // restart the store
        store = new FederatedStore();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        // check the graph is already in there from the cache
        assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME))
                .withFailMessage(String.format("Keys: %s did not contain %s", CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME), ACC_ID_2)).contains(ACC_ID_2);
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
    }

    @Test
    public void shouldInitialiseWithCache() throws StoreException {
        CacheServiceLoader.shutdown();
        assertThat(CacheServiceLoader.getDefaultService()).isNull();
        federatedProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        assertThat(CacheServiceLoader.getDefaultService()).isNull();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);
        assertThat(CacheServiceLoader.getDefaultService()).isNotNull();
    }

    @Test
    public void shouldThrowExceptionWithoutInitialisation() throws StoreException {
        federatedProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        // Given
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .properties(propertiesAlt)
                .schema(StreamUtil.openStream(FederatedStoreTest.class, SCHEMA_EDGE_BASIC_JSON))
                .build();

        CacheServiceLoader.shutdown();

        // When / Then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.addGraphs(null, TEST_USER_ID, false, graphToAdd))
                .withStackTraceContaining("Cache 'default' is not enabled, check it was initialised");
    }

    @Test
    public void shouldNotThrowExceptionWhenInitialisedWithNoCacheClassInProperties() {
        // Given
        federatedProperties = new FederatedStoreProperties();

        // When / Then
        try {
            store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);
        } catch (final StoreException e) {
            Assertions.fail("FederatedStore does not have to have a cache.");
        }
    }

    @Test
    public void shouldAddGraphsToCache() throws Exception {
        federatedProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        // Given
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .properties(propertiesAlt)
                .schema(StreamUtil.openStream(FederatedStoreTest.class, SCHEMA_EDGE_BASIC_JSON))
                .build();

        // When
        store.addGraphs(null, TEST_USER_ID, true, graphToAdd);

        // Then
        assertThat(store.getGraphs(blankUser, getCleanStrings(ACC_ID_1), new GetAllGraphIds())).hasSize(1);

        // When
        final Collection<GraphSerialisable> storeGraphs = store.getGraphs(blankUser, null, new GetAllGraphIds());

        // Then
        assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1);
        assertThat(storeGraphs).contains(graphToAdd);

        // When
        store = new FederatedStore();

        // Then
        assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1);
    }

    @Test
    public void shouldAddMultipleGraphsToCache() throws Exception {
        federatedProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);
        // Given

        final List<GraphSerialisable> graphsToAdd = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            graphsToAdd.add(new GraphSerialisable.Builder()
                    .config(new GraphConfig(ACC_ID_1 + i))
                    .properties(propertiesAlt)
                    .schema(StreamUtil.openStream(FederatedStoreTest.class, SCHEMA_EDGE_BASIC_JSON))
                    .build());
        }

        // When
        store.addGraphs(null, TEST_USER_ID, false, graphsToAdd.toArray(new GraphSerialisable[graphsToAdd.size()]));

        // Then
        for (int i = 0; i < 10; i++) {
            assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1 + i);
        }

        // When
        store = new FederatedStore();

        // Then
        for (int i = 0; i < 10; i++) {
            assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1 + i);
        }
    }

    @Test
    public void shouldAddAGraphRemoveAGraphAndBeAbleToReuseTheGraphId() throws Exception {
        // Given
        // When
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_ENTITY_BASIC_JSON);
        store.execute(new RemoveGraph.Builder()
                .graphId(ACC_ID_2)
                .build(), blankUserContext);
        addGraphWithPaths(ACC_ID_2, propertiesAlt, blankUserContext, SCHEMA_EDGE_BASIC_JSON);

        // Then
        final Collection<GraphSerialisable> graphs = store.getGraphs(blankUserContext.getUser(), getCleanStrings(ACC_ID_2), new GetAllGraphIds());
        assertThat(graphs).hasSize(1);
        JsonAssert.assertEquals(JSONSerialiser.serialise(Schema.fromJson(StreamUtil.openStream(getClass(), SCHEMA_EDGE_BASIC_JSON))),
                JSONSerialiser.serialise(graphs.iterator().next().getSchema()));
    }

    @Test
    public void shouldNotAddGraphToLibraryWhenReinitialisingFederatedStoreWithGraphFromCache() throws Exception {
        // Check cache is empty
        CacheServiceLoader.shutdown();
        federatedProperties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        assertThat(CacheServiceLoader.getDefaultService()).isNull();

        // initialise FedStore
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);

        // add something so it will be in the cache
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .properties(properties1.clone())
                .schema(StreamUtil.openStream(FederatedStoreTest.class, SCHEMA_EDGE_BASIC_JSON))
                .build();

        store.addGraphs(null, TEST_USER_ID, true, graphToAdd);

        // check is in the store
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
        // check is in the cache
        assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1);
        // check isn't in the LIBRARY
        assertThat(store.getGraphLibrary().get(ACC_ID_1)).isNull();

        // restart the store
        store = new FederatedStore();
        // initialise the FedStore
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedProperties);
        // clear and set the GraphLibrary again
        store.setGraphLibrary(library);

        // check is in the cache still
        assertThat(CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME))
                .withFailMessage(String.format("Keys: %s did not contain %s", CacheServiceLoader.getDefaultService().getAllKeysFromCache(CACHE_SERVICE_NAME), ACC_ID_1)).contains(ACC_ID_1);
        // check is in the store from the cache
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
        // check the graph isn't in the GraphLibrary
        assertThat(store.getGraphLibrary().get(ACC_ID_1)).isNull();
    }

    private List<Collection<GraphSerialisable>> populateGraphs(final int... expectedIds) throws Exception {
        final Collection<GraphSerialisable> expectedGraphs = new ArrayList<>();
        final Collection<GraphSerialisable> unexpectedGraphs = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            final GraphSerialisable tempGraph = new GraphSerialisable.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId("mockGraphId" + i)
                            .build())
                    .properties(propertiesAlt)
                    .schema(StreamUtil.openStream(FederatedStoreTest.class, SCHEMA_ENTITY_BASIC_JSON))
                    .build();
            store.addGraphs(singleton(ALL_USERS), null, true, tempGraph);

            for (final int j : expectedIds) {
                if (i == j) {
                    expectedGraphs.add(tempGraph);
                }
            }
            if (!expectedGraphs.contains(tempGraph)) {
                unexpectedGraphs.add(tempGraph);
            }
        }
        final List<Collection<GraphSerialisable>> graphLists = new ArrayList<>();
        graphLists.add(expectedGraphs);
        graphLists.add(unexpectedGraphs);
        return graphLists;
    }

    private Set<Element> getElements(final Context context) throws OperationException {
        final Iterable<? extends Element> elements = store
                .execute(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edges(store.getSchema(context, true).getEdgeGroups()) //here
                                .entities(store.getSchema(context, true).getEntityGroups()) //here 59 -> 58
                                .build())
                        .build(), context);

        return (null == elements) ? new HashSet<>() : Sets.newHashSet(elements);
    }

    private void assertContains(final Throwable e, final String format, final String... s) {
        final String expectedStr = String.format(format, (Object[]) s);
        assertThat(e.getMessage())
                .withFailMessage("\"" + e.getMessage() + "\" does not contain string \"" + expectedStr + "\"").contains(expectedStr);
    }

    private void addGraphWithIds(final String graphId, final String propertiesId, final String... schemaId)
            throws OperationException {
        final List<String> schemas = asList(schemaId);
        store.execute(new AddGraph.Builder()
                .graphId(graphId)
                .parentPropertiesId(propertiesId)
                .isPublic(true)
                .parentSchemaIds(schemas)
                .build(), blankUserContext);
    }

    private void addGraphWithPaths(final String graphId, final StoreProperties properties, Context context, final String... schemaPath)
            throws OperationException {
        final Schema.Builder schema = new Builder();
        for (final String path : schemaPath) {
            schema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(graphId)
                .storeProperties(properties)
                .isPublic(true)
                .schema(schema.build())
                .build(), blankUserContext);
    }

    private Schema getSchemaFromPath(final String path) {
        return Schema.fromJson(StreamUtil.openStream(Schema.class, path));
    }

    @Test
    public void shouldGetAllElementsWhileHasConflictingSchemasDueToDiffVertexSerialiser() throws OperationException {
        //given
        final Entity A = getEntityA();
        final Entity B = getEntityB();

        final List<Entity> expectedAB = asList(A, B);

        addElementsToNewGraph(A, "graphA", SCHEMA_ENTITY_A_JSON);
        addElementsToNewGraph(B, "graphB", SCHEMA_ENTITY_B_JSON);

        // when
        final Iterable<? extends Element> responseGraphsWithNoView = store.execute(new GetAllElements.Builder().build(), blankUserContext);
        // then
        ElementUtil.assertElementEquals(expectedAB, responseGraphsWithNoView);
    }

    @Test
    public void shouldBasicGetAllElements() throws OperationException {
        //given
        final Entity entityA = getEntityA();

        addElementsToNewGraph(entityA, "graphA", SCHEMA_ENTITY_A_JSON);

        // when
        final Iterable<? extends Element> elements = store.execute(new GetAllElements(), blankUserContext);
        // then
        assertThat(elements)
                .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
                .containsExactly(entityA);
    }

    @Test
    public void shouldGetAllElementsFromSelectedRemoteGraphWhileHasConflictingSchemasDueToDiffVertexSerialiser() throws OperationException {
        //given
        final Entity A = getEntityA();
        final Entity B = getEntityB();

        final List<Entity> expectedA = singletonList(A);
        final List<Entity> expectedB = singletonList(B);

        addElementsToNewGraph(A, "graphA", SCHEMA_ENTITY_A_JSON);
        addElementsToNewGraph(B, "graphB", SCHEMA_ENTITY_B_JSON);

        // when
        final Iterable<? extends Element> responseGraphA = store.execute(getFederatedOperation(new GetAllElements.Builder().build()).graphIdsCSV("graphA"), blankUserContext);
        final Iterable<? extends Element> responseGraphB = store.execute(getFederatedOperation(new GetAllElements.Builder().build()).graphIdsCSV("graphB"), blankUserContext);
        // then
        ElementUtil.assertElementEquals(expectedA, responseGraphA);
        ElementUtil.assertElementEquals(expectedB, responseGraphB);
    }

    @Test
    public void shouldGetAllElementsFromSelectedGraphsWithViewOfExistingEntityGroupWhileHasConflictingSchemasDueToDiffVertexSerialiser() throws OperationException {
        //given
        final Entity A = getEntityA();
        final Entity B = getEntityB();

        final List<Entity> expectedA = singletonList(A);
        final List<Entity> expectedB = singletonList(B);

        addElementsToNewGraph(A, "graphA", SCHEMA_ENTITY_A_JSON);
        addElementsToNewGraph(B, "graphB", SCHEMA_ENTITY_B_JSON);

        // when
        final Iterable<? extends Element> responseGraphAWithAView = store.execute(getFederatedOperation(new GetAllElements.Builder().view(new View.Builder().entity("entityA").build()).build()).graphIdsCSV("graphA"), blankUserContext);
        final Iterable<? extends Element> responseGraphBWithBView = store.execute(getFederatedOperation(new GetAllElements.Builder().view(new View.Builder().entity("entityB").build()).build()).graphIdsCSV("graphB"), blankUserContext);
        final Iterable<? extends Element> responseAllGraphsWithAView = store.execute(getFederatedOperation(new GetAllElements.Builder().view(new View.Builder().entity("entityA").build()).build()).graphIdsCSV("graphA,graphB"), blankUserContext);
        final Iterable<? extends Element> responseAllGraphsWithBView = store.execute(getFederatedOperation(new GetAllElements.Builder().view(new View.Builder().entity("entityB").build()).build()).graphIdsCSV("graphA,graphB"), blankUserContext);
        // then
        ElementUtil.assertElementEquals(expectedA, responseGraphAWithAView);
        ElementUtil.assertElementEquals(expectedB, responseGraphBWithBView);
        ElementUtil.assertElementEquals(expectedA, responseAllGraphsWithAView);
        ElementUtil.assertElementEquals(expectedB, responseAllGraphsWithBView);

    }

    @Test
    public void shouldFailGetAllElementsFromSelectedGraphsWithViewOfMissingEntityGroupWhileHasConflictingSchemasDueToDiffVertexSerialiser() throws OperationException {
        //given
        final Entity A = getEntityA();
        final Entity B = getEntityB();

        addElementsToNewGraph(A, "graphA", SCHEMA_ENTITY_A_JSON);
        addElementsToNewGraph(B, "graphB", SCHEMA_ENTITY_B_JSON);

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(getFederatedOperation(new GetAllElements.Builder().view(new View.Builder().entity("entityB").build()).build()).graphIdsCSV("graphA"), blankUserContext))
                .withMessage(String.format("Operation chain is invalid. Validation errors: %n" +
                        "View is not valid for graphIds:[graphA]%n" +
                        "(graphId: graphA) View for operation uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation is not valid. %n" +
                        "(graphId: graphA) Entity group entityB does not exist in the schema"));

        //when then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(getFederatedOperation(new GetAllElements.Builder().view(new View.Builder().entity("entityA").build()).build()).graphIdsCSV("graphB"), blankUserContext))
                .withMessage(String.format("Operation chain is invalid. Validation errors: %n" +
                        "View is not valid for graphIds:[graphB]%n" +
                        "(graphId: graphB) View for operation uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation is not valid. %n" +
                        "(graphId: graphB) Entity group entityA does not exist in the schema"));

        addGraphWithPaths("graphC", properties1, blankUserContext, SCHEMA_ENTITY_B_JSON);

        //when then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(getFederatedOperation(new GetAllElements.Builder().view(new View.Builder().entity("entityA").build()).build()).graphIdsCSV("graphB,graphC"), blankUserContext))
                .withMessageContaining("Operation chain is invalid. Validation errors:")
                // Order of graphIds cannot be guaranteed when all are returned
                .withMessageMatching("[\\S\\s]*View is not valid for graphIds:\\[(graphB,graphC|graphC,graphB)\\][\\S\\s]*")
                .withMessageContaining(String.format("(graphId: graphB) View for operation uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation is not valid. %n" +
                        "(graphId: graphB) Entity group entityA does not exist in the schema"))
                .withMessageContaining(String.format("(graphId: graphC) View for operation uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation is not valid. %n" +
                        "(graphId: graphC) Entity group entityA does not exist in the schema"));
    }

    @Test
    public void shouldUpdateSupportedOperations() throws Exception {
        // Given
        final Set<Class<? extends Operation>> before = store.getSupportedOperations();

        // When
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC_1, ID_SCHEMA_ENTITY);
        final Set<Class<? extends Operation>> withGraph = store.getSupportedOperations();

        store.execute(new RemoveGraph.Builder()
                .graphId(ACC_ID_1)
                .build(), blankUserContext);
        final Set<Class<? extends Operation>> afterRemove = store.getSupportedOperations();

        // Then
        assertThat(before)
                .isEqualTo(afterRemove)
                .isNotEqualTo(withGraph)
                .isSubsetOf(withGraph);
    }

    @Test
    public void shouldKeepDuplicateSupportedOperations() throws Exception {
        // Given
        final Set<Class<? extends Operation>> before = store.getSupportedOperations();

        // When
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC_1, ID_SCHEMA_ENTITY);
        final Set<Class<? extends Operation>> with1Graph = store.getSupportedOperations();

        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_EDGE);
        final Set<Class<? extends Operation>> with2Graph = store.getSupportedOperations();

        store.execute(new RemoveGraph.Builder()
                .graphId(ACC_ID_1)
                .build(), blankUserContext);
        final Set<Class<? extends Operation>> after1Remove = store.getSupportedOperations();

        // Then
        assertThat(after1Remove)
                .isNotEqualTo(before)
                .isEqualTo(with1Graph)
                .isEqualTo(with2Graph);
    }

    @Test
    public void shouldKeepFederatedStoreSupportedOperations() throws Exception {
        // Given
        final Set<Class<? extends Operation>> before = store.getSupportedOperations();

        // When
        FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.setCacheServiceNameSuffix(FED_ID_1);
        store.execute(new AddGraph.Builder()
                .graphId(FED_ID_1)
                .schema(new Schema())
                .storeProperties(properties)
                .build(), blankUserContext);

        final Set<Class<? extends Operation>> withGraph = store.getSupportedOperations();

        store.execute(new RemoveGraph.Builder()
                .graphId(FED_ID_1)
                .build(), blankUserContext);
        final Set<Class<? extends Operation>> afterRemove = store.getSupportedOperations();

        // Then
        assertThat(afterRemove)
                .contains(AddGraph.class)
                .isEqualTo(before)
                .isEqualTo(withGraph);
    }

    protected void addElementsToNewGraph(final Entity input, final String graphName, final String pathSchemaJson)
            throws OperationException {
        addGraphWithPaths(graphName, properties1, blankUserContext, pathSchemaJson);
        store.execute(getFederatedOperation(
                new AddElements.Builder()
                        .input(input)
                        .build())
                .graphIdsCSV(graphName), blankUserContext);
    }

    protected Entity getEntityB() {
        return new Entity.Builder()
                .group("entityB")
                .vertex(7)
                .build();
    }

    protected Entity getEntityA() {
        return new Entity.Builder()
                .group("entityA")
                .vertex("A")
                .build();
    }
}
