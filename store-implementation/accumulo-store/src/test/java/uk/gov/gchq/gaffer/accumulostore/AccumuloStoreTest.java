/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore;

import com.google.common.collect.Iterables;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsBetweenSetsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsInRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsWithinSetHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.ImportAccumuloKeyValueFilesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SampleDataForSplitPointsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SplitStoreHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.SummariseGroupOverRanges;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.Min;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.nio.file.Path;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.QUERY_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

public class AccumuloStoreTest {

    private static final String BYTE_ENTITY_GRAPH = "byteEntityGraph";
    private static final String GAFFER_1_GRAPH = "gaffer1Graph";
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AccumuloStoreTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloStoreTest.class, "/accumuloStoreClassicKeys.properties"));
    private static final AccumuloStore BYTE_ENTITY_STORE = new AccumuloStore();
    private static final AccumuloStore GAFFER_1_KEY_STORE = new AccumuloStore();
    private static MiniAccumuloClusterManager miniAccumuloClusterManagerByteEntity = null;
    private static MiniAccumuloClusterManager miniAccumuloClusterManagerGaffer1Key = null;

    @BeforeAll
    public static void setup(@TempDir Path tempDir) {
        miniAccumuloClusterManagerByteEntity = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
        miniAccumuloClusterManagerGaffer1Key = new MiniAccumuloClusterManager(CLASSIC_PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @BeforeEach
    public void beforeMethod() throws StoreException {
        BYTE_ENTITY_STORE.initialise(BYTE_ENTITY_GRAPH, SCHEMA, PROPERTIES);
        GAFFER_1_KEY_STORE.initialise(GAFFER_1_GRAPH, SCHEMA, CLASSIC_PROPERTIES);
    }

    @AfterAll
    public static void tearDown() {
        miniAccumuloClusterManagerByteEntity.close();
        miniAccumuloClusterManagerGaffer1Key.close();
    }

    @Test
    public void shouldNotCreateTableWhenInitialisedWithGeneralInitialiseMethod() throws StoreException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector connector = BYTE_ENTITY_STORE.getConnection();

        connector.tableOperations().delete(BYTE_ENTITY_STORE.getTableName());
        assertFalse(connector.tableOperations().exists(BYTE_ENTITY_STORE.getTableName()));

        BYTE_ENTITY_STORE.preInitialise(BYTE_ENTITY_GRAPH, SCHEMA, PROPERTIES);
        connector = BYTE_ENTITY_STORE.getConnection();
        assertFalse(connector.tableOperations().exists(BYTE_ENTITY_STORE.getTableName()));

        BYTE_ENTITY_STORE.initialise(GAFFER_1_GRAPH, SCHEMA, PROPERTIES);
        connector = BYTE_ENTITY_STORE.getConnection();
        assertTrue(connector.tableOperations().exists(BYTE_ENTITY_STORE.getTableName()));
    }

    @Test
    public void shouldCreateAStoreUsingTableName() throws Exception {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        properties.setTable("tableName");
        final AccumuloStore store = new AccumuloStore();

        // When
        store.initialise(null, SCHEMA, properties);

        // Then
        assertEquals("tableName", store.getTableName());
        assertEquals("tableName", store.getGraphId());
    }

    @Test
    public void shouldCreateAStoreUsingTableNameWithNamespace() throws Exception {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        properties.setNamespace("namespaceName");

        final AccumuloStore store = new AccumuloStore();

        // When
        store.initialise("graphId", SCHEMA, properties);

        // Then
        assertEquals("namespaceName.graphId", store.getTableName());
        assertEquals("graphId", store.getGraphId());
    }

    @Test
    public void shouldBuildGraphAndGetGraphIdFromTableName() {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        properties.setTable("tableName");

        // When
        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.schemas(getClass()))
                .storeProperties(properties)
                .build();

        // Then
        assertEquals("tableName", graph.getGraphId());
    }

    @Test
    public void shouldCreateAStoreUsingGraphIdIfItIsEqualToTableName() throws Exception {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        properties.setTable("tableName");
        final AccumuloStore store = new AccumuloStore();

        // When
        store.initialise("tableName", SCHEMA, properties);

        // Then
        assertEquals("tableName", store.getTableName());
    }

    @Test
    public void shouldThrowExceptionIfGraphIdAndTableNameAreProvidedAndDifferent() throws StoreException {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        properties.setTable("tableName");
        final AccumuloStore store = new AccumuloStore();

        // When
        IllegalArgumentException actual =
                assertThrows(IllegalArgumentException.class, () -> store.initialise("graphId", SCHEMA, properties));

        assertEquals("The table in store.properties should no longer be used. Please use a graphId instead " +
                "or for now just set the graphId to be the same value as the store.properties table.",
                actual.getMessage());
    }

    @Test
    public void shouldCreateAStoreUsingGraphId() throws Exception {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        final AccumuloStore store = new AccumuloStore();

        // When
        store.initialise("graphId", SCHEMA, properties);

        // Then
        assertEquals("graphId", store.getTableName());
    }


    @Test
    public void shouldBeAnOrderedStore() {
        assertTrue(BYTE_ENTITY_STORE.hasTrait(StoreTrait.ORDERED));
        assertTrue(GAFFER_1_KEY_STORE.hasTrait(StoreTrait.ORDERED));
    }

    @Test
    public void shouldAllowRangeScanOperationsWhenVertexSerialiserDoesPreserveObjectOrdering() throws StoreException {
        // Given
        final AccumuloStore store = new AccumuloStore();
        final Serialiser serialiser = new StringSerialiser();
        store.preInitialise(BYTE_ENTITY_GRAPH,
                new Schema.Builder()
                        .vertexSerialiser(serialiser)
                        .build(),
                PROPERTIES);

        // When
        final boolean isGetElementsInRangesSupported = store.isSupported(GetElementsInRanges.class);
        final boolean isSummariseGroupOverRangesSupported = store.isSupported(SummariseGroupOverRanges.class);

        // Then
        assertTrue(isGetElementsInRangesSupported);
        assertTrue(isSummariseGroupOverRangesSupported);
    }

    @Test
    public void shouldNotAllowRangeScanOperationsWhenVertexSerialiserDoesNotPreserveObjectOrdering() throws StoreException {
        // Given
        final AccumuloStore store = new AccumuloStore();
        final Serialiser serialiser = new CompactRawLongSerialiser();
        store.preInitialise(
                BYTE_ENTITY_GRAPH,
                new Schema.Builder()
                        .vertexSerialiser(serialiser)
                        .build(),
                PROPERTIES);

        // When
        final boolean isGetElementsInRangesSupported = store.isSupported(GetElementsInRanges.class);
        final boolean isSummariseGroupOverRangesSupported = store.isSupported(SummariseGroupOverRanges.class);

        // Then
        assertFalse(isGetElementsInRangesSupported);
        assertFalse(isSummariseGroupOverRangesSupported);
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedGaffer1() throws OperationException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(GAFFER_1_KEY_STORE);
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedByteEntity() throws OperationException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(BYTE_ENTITY_STORE);
    }

    private void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(final AccumuloStore store) throws OperationException {
        final Entity e = new Entity(TestGroups.ENTITY, "1");
        e.putProperty(TestPropertyNames.PROP_1, 1);
        e.putProperty(TestPropertyNames.PROP_2, 2);
        e.putProperty(TestPropertyNames.PROP_3, 3);
        e.putProperty(TestPropertyNames.PROP_4, 4);
        e.putProperty(TestPropertyNames.COUNT, 1);

        final User user = new User();
        final AddElements add = new AddElements.Builder()
                .input(e)
                .build();
        store.execute(add, new Context(user));

        final EntityId entityId1 = new EntitySeed("1");
        final GetElements getBySeed = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .input(entityId1)
                .build();

        CloseableIterable<? extends Element> results = null;
        try {
            results = store.execute(getBySeed, new Context(user));

            assertEquals(1, Iterables.size(results));
            assertTrue(Iterables.contains(results, e));
        } finally {
            if (results != null) {
                results.close();
            }
        }

        final GetElements getRelated = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .input(entityId1)
                .build();

        CloseableIterable<? extends Element> relatedResults = null;
        try {
            relatedResults = store.execute(getRelated, store.createContext(user));
            assertEquals(1, Iterables.size(relatedResults));
            assertTrue(Iterables.contains(relatedResults, e));

            final GetElements getRelatedWithPostAggregationFilter = new GetElements.Builder()
                    .view(new View.Builder()
                            .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                    .preAggregationFilter(new ElementFilter.Builder()
                                            .select(TestPropertyNames.PROP_1)
                                            .execute(new IsMoreThan(0))
                                            .build())
                                    .postAggregationFilter(new ElementFilter.Builder()
                                            .select(TestPropertyNames.COUNT)
                                            .execute(new IsMoreThan(6))
                                            .build())
                                    .build())
                            .build())
                    .input(entityId1)
                    .build();
            relatedResults = store.execute(getRelatedWithPostAggregationFilter, store.createContext(user));

            assertEquals(0, Iterables.size(relatedResults));
        } finally {
            if (relatedResults != null) {
                relatedResults.close();
            }
        }
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperationsGaffer1() {
        testStoreReturnsHandlersForRegisteredOperations(GAFFER_1_KEY_STORE);
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperationsByteEntity() {
        testStoreReturnsHandlersForRegisteredOperations(BYTE_ENTITY_STORE);
    }

    public void testStoreReturnsHandlersForRegisteredOperations(final AccumuloStore store) {
        OperationHandler op;
        // Then
        assertNotNull(store.getOperationHandler(Validate.class));
        op = store.getOperationHandler(AddElementsFromHdfs.class);
        assertTrue(op instanceof AddElementsFromHdfsHandler);
        op = store.getOperationHandler(GetElementsBetweenSets.class);
        assertTrue(op instanceof GetElementsBetweenSetsHandler);
        op = store.getOperationHandler(GetElementsInRanges.class);
        assertTrue(op instanceof GetElementsInRangesHandler);
        op = store.getOperationHandler(GetElementsWithinSet.class);
        assertTrue(op instanceof GetElementsWithinSetHandler);
        op = store.getOperationHandler(SplitStore.class);
        assertTrue(op instanceof SplitStoreHandler);
        op = store.getOperationHandler(SampleDataForSplitPoints.class);
        assertTrue(op instanceof SampleDataForSplitPointsHandler);
        op = store.getOperationHandler(ImportAccumuloKeyValueFiles.class);
        assertTrue(op instanceof ImportAccumuloKeyValueFilesHandler);
        op = store.getOperationHandler(GenerateElements.class);
        assertTrue(op instanceof GenerateElementsHandler);
        op = store.getOperationHandler(GenerateObjects.class);
        assertTrue(op instanceof GenerateObjectsHandler);
    }

    @Test
    public void testRequestForNullHandlerManagedGaffer1() {
        testRequestForNullHandlerManaged(GAFFER_1_KEY_STORE);
    }

    @Test
    public void testRequestForNullHandlerManagedByteEntity() {
        testRequestForNullHandlerManaged(BYTE_ENTITY_STORE);
    }

    public void testRequestForNullHandlerManaged(final AccumuloStore store) {
        final OperationHandler returnedHandler = store.getOperationHandler(null);
        assertNull(returnedHandler);
    }

    @Test
    public void testStoreTraitsGaffer1() {
        testStoreTraits(GAFFER_1_KEY_STORE);
    }

    @Test
    public void testStoreTraitsByteEntity() {
        testStoreTraits(BYTE_ENTITY_STORE);
    }

    public void testStoreTraits(final AccumuloStore store) {
        final Collection<StoreTrait> traits = store.getTraits();
        assertNotNull(traits);
        assertEquals(traits.size(), 10, "Collection size should be 10");
        assertTrue(traits.contains(INGEST_AGGREGATION), "Collection should contain INGEST_AGGREGATION trait");
        assertTrue(traits.contains(QUERY_AGGREGATION), "Collection should contain QUERY_AGGREGATION trait");
        assertTrue(traits.contains(PRE_AGGREGATION_FILTERING), "Collection should contain PRE_AGGREGATION_FILTERING trait");
        assertTrue(traits.contains(POST_AGGREGATION_FILTERING), "Collection should contain POST_AGGREGATION_FILTERING trait");
        assertTrue(traits.contains(TRANSFORMATION), "Collection should contain TRANSFORMATION trait");
        assertTrue(traits.contains(POST_TRANSFORMATION_FILTERING), "Collection should contain POST_TRANSFORMATION_FILTERING trait");
        assertTrue(traits.contains(STORE_VALIDATION), "Collection should contain STORE_VALIDATION trait");
        assertTrue(traits.contains(ORDERED), "Collection should contain ORDERED trait");
        assertTrue(traits.contains(VISIBILITY), "Collection should contain VISIBILITY trait");
    }

    @Test
    public void shouldFindInconsistentVertexSerialiser() throws StoreException {
        final Schema inconsistentSchema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("false")
                        .property(TestPropertyNames.INT, "int")
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new JavaSerialiser())
                        .aggregateFunction(new StringConcat())
                        .build())
                .type("int", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .serialiser(new JavaSerialiser())
                        .aggregateFunction(new Sum())
                        .build())
                .type("false", Boolean.class)
                .vertexSerialiser(new JavaSerialiser())
                .build();

        final AccumuloStore store = new AccumuloStore();

        // When & Then
        SchemaException actual = assertThrows(SchemaException.class,
                () -> store.preInitialise("graphId", inconsistentSchema, PROPERTIES));
        assertEquals("Vertex serialiser is inconsistent. This store requires vertices to be serialised in a consistent way.",
                actual.getMessage());

        // When & Then
        actual = assertThrows(SchemaException.class, () -> store.validateSchemas());
        assertEquals("Vertex serialiser is inconsistent. This store requires vertices to be serialised in a consistent way.",
                actual.getMessage());
    }

    @Test
    public void shouldValidateTimestampPropertyHasMaxAggregator() throws Exception {
        // Given
        final AccumuloStore store = new AccumuloStore();
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP_2)
                        .build())
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.DIRECTED_EITHER, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.TIMESTAMP_2, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .timestampProperty(TestPropertyNames.TIMESTAMP)
                .build();

        // When
        store.initialise("graphId", schema, PROPERTIES);

        // Then - no validation exceptions
    }

    @Test
    public void shouldPassSchemaValidationWhenTimestampPropertyDoesNotHaveAnAggregator() throws Exception {
        // Given
        final AccumuloStore store = new AccumuloStore();
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .aggregate(false)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP_2)
                        .aggregate(false)
                        .build())
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.DIRECTED_EITHER, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .build())
                .type(TestTypes.TIMESTAMP_2, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .build())
                .timestampProperty(TestPropertyNames.TIMESTAMP)
                .build();

        // When
        store.preInitialise("graphId", schema, PROPERTIES);

        // Then - no validation exceptions
    }

    @Test
    public void shouldFailSchemaValidationWhenTimestampPropertyDoesNotHaveMaxAggregator() throws StoreException {
        // Given
        final AccumuloStore store = new AccumuloStore();
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP_2)
                        .build())
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.TIMESTAMP_2, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Min())
                        .build())
                .timestampProperty(TestPropertyNames.TIMESTAMP)
                .build();

        // When
        SchemaException actual = assertThrows(SchemaException.class,
                () -> store.initialise("graphId", schema, PROPERTIES));

        // Then
        assertEquals("Schema is not valid. Validation errors: \n" +
                "The aggregator for the timestamp property must be set to: uk.gov.gchq.koryphe.impl.binaryoperator.Max " +
                "this cannot be overridden for this Accumulo Store, as you have told Accumulo to store this property " +
                "in the timestamp column.", actual.getMessage());
    }
}
