/*
 * Copyright 2016-2024 Crown Copyright
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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsBetweenSetsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsBetweenSetsPairsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsInRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsWithinSetHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.ImportAccumuloKeyValueFilesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SampleDataForSplitPointsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSetsPairs;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.SummariseGroupOverRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.HdfsSplitStoreFromFileHandler;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
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
import uk.gov.gchq.gaffer.store.operation.HasTrait;
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

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    private static final AccumuloProperties PROPERTIES = AccumuloProperties
            .loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(
            StreamUtil.openStream(AccumuloStoreTest.class, "/accumuloStoreClassicKeys.properties"));
    private static final AccumuloStore BYTE_ENTITY_STORE = new SingleUseMiniAccumuloStore();
    private static final AccumuloStore GAFFER_1_KEY_STORE = new SingleUseMiniAccumuloStore();

    @BeforeEach
    void beforeMethod() throws StoreException {
        BYTE_ENTITY_STORE.initialise(BYTE_ENTITY_GRAPH, SCHEMA, PROPERTIES);
        GAFFER_1_KEY_STORE.initialise(GAFFER_1_GRAPH, SCHEMA, CLASSIC_PROPERTIES);
    }

    @Test
    void shouldNotCreateTableWhenInitialisedWithGeneralInitialiseMethod()
            throws StoreException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector connector = BYTE_ENTITY_STORE.getConnection();

        connector.tableOperations().delete(BYTE_ENTITY_STORE.getTableName());
        assertThat(connector.tableOperations().exists(BYTE_ENTITY_STORE.getTableName())).isFalse();

        BYTE_ENTITY_STORE.preInitialise(BYTE_ENTITY_GRAPH, SCHEMA, PROPERTIES);
        connector = BYTE_ENTITY_STORE.getConnection();
        assertThat(connector.tableOperations().exists(BYTE_ENTITY_STORE.getTableName())).isFalse();

        BYTE_ENTITY_STORE.initialise(BYTE_ENTITY_GRAPH, SCHEMA, PROPERTIES);
        connector = BYTE_ENTITY_STORE.getConnection();
        assertThat(connector.tableOperations().exists(BYTE_ENTITY_STORE.getTableName())).isTrue();
    }

    @Test
    void shouldCreateAStoreUsingGraphId() throws Exception {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        final AccumuloStore store = new MiniAccumuloStore();

        // When
        store.initialise("graphId", SCHEMA, properties);

        // Then
        assertThat(store.getTableName()).isEqualTo("graphId");
        assertThat(store.getGraphId()).isEqualTo("graphId");
    }

    @Test
    void shouldCreateAStoreUsingGraphIdWithNamespace() throws Exception {
        // Given
        final AccumuloProperties properties = PROPERTIES.clone();
        properties.setNamespace("namespaceName");

        final AccumuloStore store = new MiniAccumuloStore();

        // When
        store.initialise("graphId", SCHEMA, properties);

        // Then
        assertThat(store.getTableName()).isEqualTo("namespaceName.graphId");
        assertThat(store.getGraphId()).isEqualTo("graphId");
    }

    @Test
    void shouldBeAnOrderedStore() throws OperationException {
        assertThat(BYTE_ENTITY_STORE
                .execute(new HasTrait.Builder().trait(StoreTrait.ORDERED).currentTraits(false).build(), new Context()))
                .isTrue();
        assertThat(GAFFER_1_KEY_STORE
                .execute(new HasTrait.Builder().trait(StoreTrait.ORDERED).currentTraits(false).build(), new Context()))
                .isTrue();
    }

    @Test
    void shouldAllowRangeScanOperationsWhenVertexSerialiserDoesPreserveObjectOrdering() throws StoreException {
        // Given
        final AccumuloStore store = new AccumuloStore();
        final Serialiser<?, ?> serialiser = new StringSerialiser();
        store.preInitialise(BYTE_ENTITY_GRAPH,
                new Schema.Builder()
                        .vertexSerialiser(serialiser)
                        .build(),
                PROPERTIES);

        // When
        final boolean isGetElementsInRangesSupported = store.isSupported(GetElementsInRanges.class);
        final boolean isSummariseGroupOverRangesSupported = store.isSupported(SummariseGroupOverRanges.class);

        // Then
        assertThat(isGetElementsInRangesSupported).isTrue();
        assertThat(isSummariseGroupOverRangesSupported).isTrue();
    }

    @Test
    void shouldNotAllowRangeScanOperationsWhenVertexSerialiserDoesNotPreserveObjectOrdering()
            throws StoreException {
        // Given
        final AccumuloStore store = new AccumuloStore();
        final Serialiser<?, ?> serialiser = new CompactRawLongSerialiser();
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
        assertThat(isGetElementsInRangesSupported).isFalse();
        assertThat(isSummariseGroupOverRangesSupported).isFalse();
    }

    @Test
    void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedGaffer1() throws OperationException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(GAFFER_1_KEY_STORE);
    }

    @Test
    void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedByteEntity() throws OperationException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(BYTE_ENTITY_STORE);
    }

    private void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(final AccumuloStore store)
            throws OperationException {
        final Element e = new Entity(TestGroups.ENTITY, "1");
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

        final Iterable<? extends Element> results = store.execute(getBySeed, new Context(user));

        assertThat(results).hasSize(1);
        assertThat(results).asInstanceOf(InstanceOfAssertFactories.iterable(Element.class)).contains(e);

        final GetElements getRelated = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .input(entityId1)
                .build();

        Iterable<? extends Element> relatedResults = store.execute(getRelated, store.createContext(user));
        assertThat(relatedResults).hasSize(1);
        assertThat(relatedResults).asInstanceOf(InstanceOfAssertFactories.iterable(Element.class)).contains(e);

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
        assertThat(relatedResults).hasSize(0);
    }

    @Test
    void testStoreReturnsHandlersForRegisteredOperationsGaffer1() {
        testStoreReturnsHandlersForRegisteredOperations(GAFFER_1_KEY_STORE);
    }

    @Test
    void testStoreReturnsHandlersForRegisteredOperationsByteEntity() {
        testStoreReturnsHandlersForRegisteredOperations(BYTE_ENTITY_STORE);
    }

    void testStoreReturnsHandlersForRegisteredOperations(final AccumuloStore store) {
        OperationHandler<?> op;
        // Then
        assertThat(store.getOperationHandler(Validate.class)).isNotNull();

        op = store.getOperationHandler(AddElementsFromHdfs.class);
        assertThat(op).isInstanceOf(AddElementsFromHdfsHandler.class);
        op = store.getOperationHandler(GetElementsBetweenSets.class);
        assertThat(op).isInstanceOf(GetElementsBetweenSetsHandler.class);
        op = store.getOperationHandler(GetElementsBetweenSetsPairs.class);
        assertThat(op).isInstanceOf(GetElementsBetweenSetsPairsHandler.class);
        op = store.getOperationHandler(GetElementsInRanges.class);
        assertThat(op).isInstanceOf(GetElementsInRangesHandler.class);
        op = store.getOperationHandler(GetElementsWithinSet.class);
        assertThat(op).isInstanceOf(GetElementsWithinSetHandler.class);
        op = store.getOperationHandler(SplitStoreFromFile.class);
        assertThat(op).isInstanceOf(HdfsSplitStoreFromFileHandler.class);
        op = store.getOperationHandler(SampleDataForSplitPoints.class);
        assertThat(op).isInstanceOf(SampleDataForSplitPointsHandler.class);
        op = store.getOperationHandler(ImportAccumuloKeyValueFiles.class);
        assertThat(op).isInstanceOf(ImportAccumuloKeyValueFilesHandler.class);
        op = store.getOperationHandler(GenerateElements.class);
        assertThat(op).isInstanceOf(GenerateElementsHandler.class);
        op = store.getOperationHandler(GenerateObjects.class);
        assertThat(op).isInstanceOf(GenerateObjectsHandler.class);
    }

    @Test
    void testRequestForNullHandlerManagedGaffer1() {
        testRequestForNullHandlerManaged(GAFFER_1_KEY_STORE);
    }

    @Test
    void testRequestForNullHandlerManagedByteEntity() {
        testRequestForNullHandlerManaged(BYTE_ENTITY_STORE);
    }

    void testRequestForNullHandlerManaged(final AccumuloStore store) {
        final OperationHandler<?> returnedHandler = store.getOperationHandler(null);
        assertThat(returnedHandler).isNull();
    }

    @Test
    void shouldHaveSupportedStoreTraits() {
        final Collection<StoreTrait> traits = AccumuloStore.TRAITS;
        assertThat(traits).isNotNull();
        assertThat(traits).withFailMessage("Collection size should be 10").hasSize(10);

        assertThat(traits).withFailMessage("Collection should contain INGEST_AGGREGATION trait")
                .contains(INGEST_AGGREGATION)
                .withFailMessage("Collection should contain QUERY_AGGREGATION trait").contains(QUERY_AGGREGATION)
                .withFailMessage("Collection should contain PRE_AGGREGATION_FILTERING trait")
                .contains(PRE_AGGREGATION_FILTERING)
                .withFailMessage("Collection should contain POST_AGGREGATION_FILTERING trait")
                .contains(POST_AGGREGATION_FILTERING)
                .withFailMessage("Collection should contain TRANSFORMATION trait").contains(TRANSFORMATION)
                .withFailMessage("Collection should contain POST_TRANSFORMATION_FILTERING trait")
                .contains(POST_TRANSFORMATION_FILTERING)
                .withFailMessage("Collection should contain STORE_VALIDATION trait").contains(STORE_VALIDATION)
                .withFailMessage("Collection should contain ORDERED trait").contains(ORDERED)
                .withFailMessage("Collection should contain VISIBILITY trait").contains(VISIBILITY);
    }

    @Test
    void shouldFindInconsistentVertexSerialiser() throws StoreException {
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
        assertThatExceptionOfType(SchemaException.class)
                .isThrownBy(() -> store.preInitialise("graphId", inconsistentSchema, PROPERTIES))
                .withMessage(
                        "Vertex serialiser is inconsistent. This store requires vertices to be serialised in a consistent way.");

        // When & Then
        assertThatExceptionOfType(SchemaException.class)
                .isThrownBy(() -> store.validateSchemas())
                .withMessage(
                        "Vertex serialiser is inconsistent. This store requires vertices to be serialised in a consistent way.");
    }

    @Test
    void shouldValidateTimestampPropertyHasMaxAggregator() throws Exception {
        // Given
        final AccumuloStore store = new MiniAccumuloStore();
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
                .config(AccumuloStoreConstants.TIMESTAMP_PROPERTY, TestPropertyNames.TIMESTAMP)
                .build();

        // When
        store.initialise("graphId", schema, PROPERTIES);

        // Then - no validation exceptions
    }

    @Test
    void shouldPassSchemaValidationWhenTimestampPropertyDoesNotHaveAnAggregator() throws Exception {
        // Given
        final AccumuloStore store = new MiniAccumuloStore();
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

                .build();

        // When
        store.preInitialise("graphId", schema, PROPERTIES);

        // Then - no validation exceptions
    }

    @Test
    void shouldFailSchemaValidationWhenTimestampPropertyDoesNotHaveMaxAggregator() throws StoreException {
        // Given
        final AccumuloStore store = new MiniAccumuloStore();
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
                .config(AccumuloStoreConstants.TIMESTAMP_PROPERTY, TestPropertyNames.TIMESTAMP)
                .build();

        // When / Then
        final String expectedMessage = "The aggregator for the timestamp property must be set to: uk.gov.gchq.koryphe.impl.binaryoperator.Max";

        assertThatExceptionOfType(SchemaException.class)
                .isThrownBy(() -> store.initialise("graphId", schema, PROPERTIES))
                .withMessageContaining(expectedMessage);
    }

     @Test
     void shouldThrowWhenDeletingNull() {
        final AccumuloStore store = new MiniAccumuloStore();
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> store.deleteElements(null))
                .withMessageContaining("Could not find any elements to delete");
     }

     @Test
     void shouldDeleteElements() throws Exception {
        try (MockedStatic<TableUtils> utils = Mockito.mockStatic(TableUtils.class)) {
            BatchWriter writer = Mockito.mock(BatchWriter.class);
            utils.when(() -> TableUtils.createBatchWriter(any())).thenReturn(writer);

            final AccumuloStore store = new MiniAccumuloStore();
            store.initialise("graphId", SCHEMA, PROPERTIES);

            store.deleteElements(Arrays.asList(new Entity("BasicEntity", "1")));
            verify(writer, times(1)).addMutation(any());
        }
     }

     @Test
     void shouldDoNothingWhenCannotConvert() throws Exception {
        try (MockedStatic<TableUtils> utils = Mockito.mockStatic(TableUtils.class)) {
            BatchWriter writer = Mockito.mock(BatchWriter.class);
            utils.when(() -> TableUtils.createBatchWriter(any())).thenReturn(writer);

            final AccumuloStore store = new MiniAccumuloStore();
            store.initialise("graphId", SCHEMA, PROPERTIES);

            store.deleteElements(Arrays.asList(new Entity("blah", 1)));
            verify(writer, never()).addMutation(any());
        }
     }

     @Test
     void shouldDoNothingWhenCannotMutate() throws Exception {
        try (MockedStatic<TableUtils> utils = Mockito.mockStatic(TableUtils.class)) {
            BatchWriter writer = Mockito.mock(BatchWriter.class);
            MutationsRejectedException e = new MutationsRejectedException((Instance) null, Collections.emptyList(), Collections.emptyMap(), Collections.emptyList(), 0, null);
            doThrow(e).when(writer).addMutation(any());
            utils.when(() -> TableUtils.createBatchWriter(any())).thenReturn(writer);

            final AccumuloStore store = new MiniAccumuloStore();
            store.initialise("graphId", SCHEMA, PROPERTIES);

            assertThatNoException().isThrownBy(() -> store.deleteElements(Arrays.asList(new Entity("BasicEntity", "1"))));
        }
     }

     @Test
     void shouldDoNothingWhenCannotClose() throws Exception {
        try (MockedStatic<TableUtils> utils = Mockito.mockStatic(TableUtils.class)) {
            BatchWriter writer = Mockito.mock(BatchWriter.class);
            MutationsRejectedException e = new MutationsRejectedException((Instance) null, Collections.emptyList(), Collections.emptyMap(), Collections.emptyList(), 0, null);
            doThrow(e).when(writer).close();
            utils.when(() -> TableUtils.createBatchWriter(any())).thenReturn(writer);

            final AccumuloStore store = new MiniAccumuloStore();
            store.initialise("graphId", SCHEMA, PROPERTIES);

            assertThatNoException().isThrownBy(() -> store.deleteElements(Arrays.asList(new Entity("BasicEntity", "1"))));
        }
     }

     @Test
     void shouldStoreTableCreatedTimeProperty() throws Exception {
         // Given
         final AccumuloProperties properties = PROPERTIES.clone();
         String graphId = "graphId";

         final AccumuloStore store = new MiniAccumuloStore();
         // When

         store.initialise(graphId, SCHEMA, properties);
         LocalDateTime dateTime = LocalDateTime.now();
         LocalDateTime storeDateTime = LocalDateTime.parse(store.getCreatedTime());
         // Then
         assertThat(storeDateTime).isBeforeOrEqualTo(dateTime);
     }
     @Test
     void shouldThrowGafferRuntimeExceptionForGetCreatedTimeIfTableNotFound() throws Exception {
        // Given
        final AccumuloStore store = new MiniAccumuloStore();
        store.initialise("graphId", SCHEMA, PROPERTIES);

        // When
        store.getConnection().tableOperations().delete("graphId");

        // Then
        assertThatExceptionOfType(GafferRuntimeException.class).isThrownBy(() -> store.getCreatedTime()).withMessage("Error getting timestamp.");
     }

     @Test
     void shouldThrowGafferRuntimeExceptionIfTableCreatedTimePropertyNotFound() throws Exception {
        // Given
        final AccumuloStore store = new MiniAccumuloStore();
        store.initialise("graphId", SCHEMA, PROPERTIES);

        // When
        store.getConnection().tableOperations().removeProperty("graphId", AccumuloProperties.TABLE_CREATED_TIME);

        // Then
        assertThatExceptionOfType(GafferRuntimeException.class).isThrownBy(() -> store.getCreatedTime()).withMessage("Timestamp not found on table");

     }
}
