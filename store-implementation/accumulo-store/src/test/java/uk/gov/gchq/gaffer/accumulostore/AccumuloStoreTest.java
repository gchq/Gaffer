/*
 * Copyright 2016 Crown Copyright
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
    private static SingleUseMockAccumuloStore byteEntityStore;
    private static SingleUseMockAccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AccumuloStoreTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloStoreTest.class, "/accumuloStoreClassicKeys.properties"));

    @BeforeClass
    public static void setup() throws StoreException, AccumuloException, AccumuloSecurityException, IOException {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
    }

    @Before
    public void beforeMethod() throws StoreException, IOException {
        byteEntityStore.initialise(BYTE_ENTITY_GRAPH, SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise(GAFFER_1_GRAPH, SCHEMA, CLASSIC_PROPERTIES);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
    }

    @Test
    public void shouldNotCreateTableWhenInitialisedWithGeneralInitialiseMethod() throws StoreException, IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector connector = byteEntityStore.getConnection();

        connector.tableOperations().delete(byteEntityStore.getTableName());
        assertFalse(connector.tableOperations().exists(byteEntityStore.getTableName()));

        byteEntityStore.preInitialise(BYTE_ENTITY_GRAPH, SCHEMA, PROPERTIES);
        connector = byteEntityStore.getConnection();
        assertFalse(connector.tableOperations().exists(byteEntityStore.getTableName()));

        byteEntityStore.initialise(GAFFER_1_GRAPH, SCHEMA, PROPERTIES);
        connector = byteEntityStore.getConnection();
        assertTrue(connector.tableOperations().exists(byteEntityStore.getTableName()));
    }

    @Test
    public void shouldCreateAStoreUsingTableName() throws Exception {
        // Given
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
        properties.setTable("tableName");
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();

        // When
        store.initialise(null, SCHEMA, properties);

        // Then
        assertEquals("tableName", store.getTableName());
        assertEquals("tableName", store.getGraphId());
    }

    @Test
    public void shouldBuildGraphAndGetGraphIdFromTableName() throws Exception {
        // Given
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
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
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
        properties.setTable("tableName");
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();

        // When
        store.initialise("tableName", SCHEMA, properties);

        // Then
        assertEquals("tableName", store.getTableName());
    }

    @Test
    public void shouldThrowExceptionIfGraphIdAndTableNameAreProvidedAndDifferent() throws Exception {
        // Given
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
        properties.setTable("tableName");
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();

        // When / Then
        try {
            store.initialise("graphId", SCHEMA, properties);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldCreateAStoreUsingGraphId() throws Exception {
        // Given
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();

        // When
        store.initialise("graphId", SCHEMA, properties);

        // Then
        assertEquals("graphId", store.getTableName());
    }


    @Test
    public void shouldBeAnOrderedStore() {
        assertTrue(byteEntityStore.hasTrait(StoreTrait.ORDERED));
        assertTrue(gaffer1KeyStore.hasTrait(StoreTrait.ORDERED));
    }

    @Test
    public void shouldAllowRangeScanOperationsWhenVertexSerialiserDoesPreserveObjectOrdering() throws StoreException {
        // Given
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        final Serialiser serialiser = new StringSerialiser();
        store.initialise(
                BYTE_ENTITY_GRAPH,
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
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        final Serialiser serialiser = new CompactRawLongSerialiser();
        store.initialise(
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
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedGaffer1() throws OperationException, StoreException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(gaffer1KeyStore);
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedByteEntity() throws OperationException, StoreException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(byteEntityStore);
    }

    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(final AccumuloStore store) throws OperationException, StoreException {
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
        store.execute(add, store.createContext(user));

        final EntityId entityId1 = new EntitySeed("1");
        final GetElements getBySeed = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .input(entityId1)
                .build();
        final CloseableIterable<? extends Element> results = store.execute(getBySeed, store.createContext(user));

        assertEquals(1, Iterables.size(results));
        assertTrue(Iterables.contains(results, e));

        final GetElements getRelated = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .input(entityId1)
                .build();
        CloseableIterable<? extends Element> relatedResults = store.execute(getRelated, store.createContext(user));
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
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperationsGaffer1() throws OperationException, StoreException {
        testStoreReturnsHandlersForRegisteredOperations(gaffer1KeyStore);
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperationsByteEntity() throws OperationException, StoreException {
        testStoreReturnsHandlersForRegisteredOperations(byteEntityStore);
    }

    public void testStoreReturnsHandlersForRegisteredOperations(final SingleUseMockAccumuloStore store) throws StoreException {
        // Then
        assertNotNull(store.getOperationHandlerExposed(Validate.class));
        assertTrue(store.getOperationHandlerExposed(AddElementsFromHdfs.class) instanceof AddElementsFromHdfsHandler);
        assertTrue(store.getOperationHandlerExposed(GetElementsBetweenSets.class) instanceof GetElementsBetweenSetsHandler);
        assertTrue(store.getOperationHandlerExposed(GetElementsInRanges.class) instanceof GetElementsInRangesHandler);
        assertTrue(store.getOperationHandlerExposed(GetElementsWithinSet.class) instanceof GetElementsWithinSetHandler);
        assertTrue(store.getOperationHandlerExposed(SplitStore.class) instanceof SplitStoreHandler);
        assertTrue(store.getOperationHandlerExposed(SampleDataForSplitPoints.class) instanceof SampleDataForSplitPointsHandler);
        assertTrue(store.getOperationHandlerExposed(ImportAccumuloKeyValueFiles.class) instanceof ImportAccumuloKeyValueFilesHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);
    }

    @Test
    public void testRequestForNullHandlerManagedGaffer1() throws OperationException {
        testRequestForNullHandlerManaged(gaffer1KeyStore);
    }

    @Test
    public void testRequestForNullHandlerManagedByteEntity() throws OperationException {
        testRequestForNullHandlerManaged(byteEntityStore);
    }

    public void testRequestForNullHandlerManaged(final SingleUseMockAccumuloStore store) {
        final OperationHandler returnedHandler = store.getOperationHandlerExposed(null);
        assertNull(returnedHandler);
    }


    @Test
    public void testStoreTraitsGaffer1() throws OperationException {
        testStoreTraits(gaffer1KeyStore);
    }

    @Test
    public void testStoreTraitsByteEntity() throws OperationException {
        testStoreTraits(byteEntityStore);
    }

    public void testStoreTraits(final AccumuloStore store) {
        final Collection<StoreTrait> traits = store.getTraits();
        assertNotNull(traits);
        assertTrue("Collection size should be 8", traits.size() == 9);
        assertTrue("Collection should contain INGEST_AGGREGATION trait", traits.contains(INGEST_AGGREGATION));
        assertTrue("Collection should contain QUERY_AGGREGATION trait", traits.contains(QUERY_AGGREGATION));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
        assertTrue("Collection should contain ORDERED trait", traits.contains(ORDERED));
        assertTrue("Collection should contain VISIBILITY trait", traits.contains(VISIBILITY));
    }

    @Test(expected = SchemaException.class)
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

        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();

        store.preInitialise("graphId", inconsistentSchema, PROPERTIES);
        try {
            store.validateSchemas();
            fail("Exception expected");
        } catch (final SchemaException e) {
            assert(e.getMessage().contains("serialisers to be consistent."));
        }
    }
}

