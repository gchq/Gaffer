/*
 * Copyright 2016-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.LazyEntity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
import uk.gov.gchq.gaffer.jobtracker.Repeat;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.impl.GetVariable;
import uk.gov.gchq.gaffer.operation.impl.GetVariables;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.Reduce;
import uk.gov.gchq.gaffer.operation.impl.SetVariable;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.ValidateOperationChain;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.CancelScheduledJob;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.impl.output.ToArray;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToSingletonList;
import uk.gov.gchq.gaffer.operation.impl.output.ToStream;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.tostring.StringToStringSerialiser;
import uk.gov.gchq.gaffer.store.Store.ScheduledJobRunnable;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.ExportToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.GetSetExportHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToSetHandler;
import uk.gov.gchq.gaffer.store.optimiser.AbstractOperationChainOptimiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

public class StoreTest {
    private final User user = new User("user01");
    private final Context context = new Context(user);

    private OperationHandler<AddElements> addElementsHandler;
    private OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getElementsHandler;
    private OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getAllElementsHandler;
    private OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler;
    private OperationHandler<Validate> validateHandler;
    private Schema schema;
    private SchemaOptimiser schemaOptimiser;
    private JobTracker jobTracker;
    private OperationHandler<ExportToGafferResultCache> exportToGafferResultCacheHandler;
    private OperationHandler<GetGafferResultCacheExport> getGafferResultCacheExportHandler;
    private StoreImpl store;
    private OperationChainValidator operationChainValidator;

    @BeforeEach
    public void setup() {
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_CLASS_KEY);
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_MODULES);
        JSONSerialiser.update();

        schemaOptimiser = mock(SchemaOptimiser.class);
        operationChainValidator = mock(OperationChainValidator.class);
        store = new StoreImpl();
        given(operationChainValidator.validate(any(OperationChain.class), any(User.class), any(Store.class))).willReturn(new ValidationResult());
        addElementsHandler = mock(OperationHandler.class);
        getElementsHandler = mock(OutputOperationHandler.class);
        getAllElementsHandler = mock(OutputOperationHandler.class);
        getAdjacentIdsHandler = mock(OutputOperationHandler.class);
        validateHandler = mock(OperationHandler.class);
        exportToGafferResultCacheHandler = mock(OperationHandler.class);
        getGafferResultCacheExportHandler = mock(OperationHandler.class);
        jobTracker = mock(JobTracker.class);
        schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringSerialiser())
                        .aggregateFunction(new StringConcat())
                        .build())
                .type("true", Boolean.class)
                .build();
    }

    @AfterEach
    public void after() {
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_CLASS_KEY);
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_MODULES);
        JSONSerialiser.update();
    }


    @Test
    public void shouldThrowExceptionIfGraphIdIsNull() throws Exception {
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        assertThatIllegalArgumentException().isThrownBy(() -> store.initialise(null, schema, properties)).extracting("message").isNotNull();
    }

    @Test
    public void shouldThrowExceptionWhenPropertyIsNotSerialisable() throws StoreException {
        // Given
        final Schema mySchema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "invalidType")
                        .build())
                .type("invalidType", new TypeDefinition.Builder()
                        .clazz(Object.class)
                        .serialiser(new StringSerialiser())
                        .build())
                .build();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        // When
        assertThatExceptionOfType(SchemaException.class).isThrownBy(() -> store.initialise("graphId", mySchema, properties)).extracting("message").isNotNull();
    }

    @Test
    public void shouldCreateStoreWithValidSchemasAndRegisterOperations() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        final OperationHandler<AddElements> addElementsHandlerOverridden = mock(OperationHandler.class);
        final OperationDeclarations opDeclarations = new OperationDeclarations.Builder()
                .declaration(new OperationDeclaration.Builder()
                        .operation(AddElements.class)
                        .handler(addElementsHandlerOverridden)
                        .build())
                .build();
        given(properties.getOperationDeclarations()).willReturn(opDeclarations);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        // When
        store.initialise("graphId", schema, properties);

        // Then
        assertNotNull(store.getOperationHandlerExposed(Validate.class));
        assertSame(addElementsHandlerOverridden, store.getOperationHandlerExposed(AddElements.class));

        assertSame(getAllElementsHandler, store.getOperationHandlerExposed(GetAllElements.class));

        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);

        assertTrue(store.getOperationHandlerExposed(CountGroups.class) instanceof CountGroupsHandler);
        assertTrue(store.getOperationHandlerExposed(ToSet.class) instanceof ToSetHandler);

        assertTrue(store.getOperationHandlerExposed(ExportToSet.class) instanceof ExportToSetHandler);
        assertTrue(store.getOperationHandlerExposed(GetSetExport.class) instanceof GetSetExportHandler);

        assertEquals(1, store.getCreateOperationHandlersCallCount());
        assertSame(schema, store.getSchema());
        assertSame(properties, store.getProperties());
        verify(schemaOptimiser).optimise(store.getSchema(), true);
    }

    @Test
    public void shouldDelegateDoOperationToOperationHandler() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final AddElements addElements = new AddElements();
        store.initialise("graphId", schema, properties);

        // When
        store.execute(addElements, context);

        // Then
        verify(addElementsHandler).doOperation(addElements, context, store);
    }

    @Test
    public void shouldCloseOperationIfResultIsNotCloseable() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final Operation operation = mock(Operation.class);
        final StoreImpl store = new StoreImpl();
        store.initialise("graphId", schema, properties);

        // When
        store.handleOperation(operation, context);

        // Then
        verify(operation).close();
    }

    @Test
    public void shouldCloseOperationIfExceptionThrown() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final Operation operation = mock(Operation.class);
        final StoreImpl store = new StoreImpl();
        final OperationHandler opHandler = mock(OperationHandler.class);
        store.addOperationHandler(Operation.class, opHandler);
        store.initialise("graphId", schema, properties);

        given(opHandler.doOperation(operation, context, store)).willThrow(new RuntimeException());

        // When / Then
        try {
            store.handleOperation(operation, context);
        } catch (final Exception e) {
            verify(operation).close();
        }
    }

    @Test
    public void shouldThrowExceptionIfOperationChainIsInvalid() throws OperationException, StoreException {
        // Given
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        final OperationChain opChain = new OperationChain();
        final StoreImpl store = new StoreImpl();

        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(schema.validate()).willReturn(new ValidationResult());
        ValidationResult validationResult = new ValidationResult();
        validationResult.addError("error");
        given(operationChainValidator.validate(opChain, user, store)).willReturn(validationResult);
        store.initialise("graphId", schema, properties);

        // When / Then
        try {
            store.execute(opChain, context);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            verify(operationChainValidator).validate(opChain, user, store);
            assertTrue(e.getMessage().contains("Operation chain"));
        }
    }

    @Test
    public void shouldCallDoUnhandledOperationWhenDoOperationWithUnknownOperationClass() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        final Operation operation = new SetVariable.Builder().variableName("aVariable").input("inputString").build();
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        store.initialise("graphId", schema, properties);

        // When
        store.execute(operation, context);

        // Then
        assertEquals(1, store.getDoUnhandledOperationCalls().size());
        assertSame(operation, store.getDoUnhandledOperationCalls().get(0));
    }

    @Test
    public void shouldFullyLoadLazyElement() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        final LazyEntity lazyElement = mock(LazyEntity.class);
        final Entity entity = mock(Entity.class);
        final Store store = new StoreImpl();
        given(lazyElement.getGroup()).willReturn(TestGroups.ENTITY);
        given(lazyElement.getElement()).willReturn(entity);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        store.initialise("graphId", schema, properties);

        // When
        final Element result = store.populateElement(lazyElement);

        // Then
        assertSame(entity, result);
        verify(lazyElement).getGroup();
        verify(lazyElement).getProperty(TestPropertyNames.PROP_1);
        verify(lazyElement).getIdentifier(IdentifierType.VERTEX);
    }

    @Test
    public void shouldHandleMultiStepOperations() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        final CloseableIterable getElementsResult = mock(CloseableIterable.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        final AddElements addElements1 = new AddElements();
        final GetElements getElements = new GetElements();
        final OperationChain<CloseableIterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(addElements1)
                .then(getElements)
                .build();


        given(addElementsHandler.doOperation(addElements1, context, store)).willReturn(null);
        given(getElementsHandler.doOperation(getElements, context, store))
                .willReturn(getElementsResult);

        store.initialise("graphId", schema, properties);

        // When
        final CloseableIterable<? extends Element> result = store.execute(opChain, context);

        // Then
        assertSame(getElementsResult, result);
    }

    @Test
    public void shouldReturnAllSupportedOperations() throws Exception {
        // Given
        final Properties cacheProperties = new Properties();
        cacheProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());
        CacheServiceLoader.initialise(cacheProperties);

        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        store.initialise("graphId", schema, properties);

        // When
        final List<Class<? extends Operation>> supportedOperations = Lists.newArrayList(store.getSupportedOperations());

        // Then
        assertNotNull(supportedOperations);

        final List<Class<? extends Operation>> expectedOperations = Lists.newArrayList(
                AddElements.class,
                GetElements.class,
                GetAdjacentIds.class,
                GetAllElements.class,

                mock(AddElements.class).getClass(),
                mock(GetElements.class).getClass(),
                mock(GetAdjacentIds.class).getClass(),

                // Export
                ExportToSet.class,
                GetSetExport.class,
                GetExports.class,
                ExportToGafferResultCache.class,
                GetGafferResultCacheExport.class,

                // Jobs
                GetJobDetails.class,
                GetAllJobDetails.class,
                GetJobResults.class,

                // Output
                ToArray.class,
                ToEntitySeeds.class,
                ToList.class,
                ToMap.class,
                ToCsv.class,
                ToSet.class,
                ToStream.class,
                ToVertices.class,

                // Named Operations
                NamedOperation.class,
                AddNamedOperation.class,
                GetAllNamedOperations.class,
                DeleteNamedOperation.class,

                // Named View
                AddNamedView.class,
                GetAllNamedViews.class,
                DeleteNamedView.class,

                // ElementComparison
                Max.class,
                Min.class,
                Sort.class,

                // Validation
                ValidateOperationChain.class,

                // Algorithm
                GetWalks.class,

                // OperationChain
                OperationChain.class,
                OperationChainDAO.class,

                // Other
                GenerateElements.class,
                GenerateObjects.class,
                Validate.class,
                Count.class,
                CountGroups.class,
                Limit.class,
                DiscardOutput.class,
                GetSchema.class,
                Map.class,
                If.class,
                GetTraits.class,
                While.class,
                Join.class,
                ToSingletonList.class,
                ForEach.class,
                Reduce.class,
                CancelScheduledJob.class,

                // Function
                Filter.class,
                Transform.class,
                Aggregate.class,

                // Context variables
                SetVariable.class,
                GetVariable.class,
                GetVariables.class
        );

        expectedOperations.sort(Comparator.comparing(Class::getName));
        supportedOperations.sort(Comparator.comparing(Class::getName));
        assertEquals(expectedOperations, supportedOperations);
    }

    @Test
    public void shouldReturnAllSupportedOperationsWhenJobTrackerIsDisabled() throws Exception {
        // Given
        final Properties cacheProperties = new Properties();
        cacheProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());
        CacheServiceLoader.initialise(cacheProperties);

        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(false);
        store.initialise("graphId", schema, properties);

        // When
        final List<Class<? extends Operation>> supportedOperations = Lists.newArrayList(store.getSupportedOperations());

        // Then
        assertNotNull(supportedOperations);

        final List<Class<? extends Operation>> expectedOperations = Lists.newArrayList(
                AddElements.class,
                GetElements.class,
                GetAdjacentIds.class,
                GetAllElements.class,

                mock(AddElements.class).getClass(),
                mock(GetElements.class).getClass(),
                mock(GetAdjacentIds.class).getClass(),

                // Export
                ExportToSet.class,
                GetSetExport.class,
                GetExports.class,
                ExportToGafferResultCache.class,
                GetGafferResultCacheExport.class,

                // Jobs are disabled

                // Output
                ToArray.class,
                ToEntitySeeds.class,
                ToList.class,
                ToMap.class,
                ToCsv.class,
                ToSet.class,
                ToStream.class,
                ToVertices.class,

                // Named Operations
                NamedOperation.class,
                AddNamedOperation.class,
                GetAllNamedOperations.class,
                DeleteNamedOperation.class,

                // Named View
                AddNamedView.class,
                GetAllNamedViews.class,
                DeleteNamedView.class,

                // ElementComparison
                Max.class,
                Min.class,
                Sort.class,

                // Validation
                ValidateOperationChain.class,

                // Algorithm
                GetWalks.class,

                // OperationChain
                OperationChain.class,
                OperationChainDAO.class,

                // Other
                GenerateElements.class,
                GenerateObjects.class,
                Validate.class,
                Count.class,
                CountGroups.class,
                Limit.class,
                DiscardOutput.class,
                GetSchema.class,
                GetTraits.class,
                Map.class,
                If.class,
                While.class,
                Join.class,
                ToSingletonList.class,
                ForEach.class,
                Reduce.class,
                CancelScheduledJob.class,

                // Function
                Filter.class,
                Transform.class,
                Aggregate.class,

                // Context variables
                SetVariable.class,
                GetVariable.class,
                GetVariables.class
        );

        expectedOperations.sort(Comparator.comparing(Class::getName));
        supportedOperations.sort(Comparator.comparing(Class::getName));
        assertEquals(expectedOperations, supportedOperations);
    }

    @Test
    public void shouldReturnTrueWhenOperationSupported() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise("graphId", schema, properties);

        // WHen
        final Set<Class<? extends Operation>> supportedOperations = store.getSupportedOperations();
        for (final Class<? extends Operation> operationClass : supportedOperations) {
            final boolean isOperationClassSupported = store.isSupported(operationClass);

            // Then
            assertTrue(isOperationClassSupported);
        }
    }

    @Test
    public void shouldReturnFalseWhenUnsupportedOperationRequested() throws
            Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise("graphId", schema, properties);

        // When
        final boolean supported = store.isSupported(Operation.class);

        // Then
        assertFalse(supported);
    }

    @Test
    public void shouldHandleNullOperationSupportRequest() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise("graphId", schema, properties);

        // When
        final boolean supported = store.isSupported(null);

        // Then
        assertFalse(supported);
    }

    @Test
    public void shouldExecuteOperationChainJob() throws OperationException, InterruptedException, StoreException {
        // Given
        final Operation operation = new GetVariables.Builder().variableNames(Lists.newArrayList()).build();
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(operation)
                .then(new ExportToGafferResultCache())
                .build();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise("graphId", schema, properties);

        // When
        final JobDetail resultJobDetail = store.executeJob(opChain, context);

        // Then
        Thread.sleep(1000);
        final ArgumentCaptor<JobDetail> jobDetail = ArgumentCaptor.forClass(JobDetail.class);
        verify(jobTracker, times(2)).addOrUpdateJob(jobDetail.capture(), eq(user));
        assertEquals(jobDetail.getAllValues().get(0), resultJobDetail);
        assertEquals(JobStatus.FINISHED, jobDetail.getAllValues().get(1).getStatus());

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class), contextCaptor.capture(), eq(store));
        assertSame(user, contextCaptor.getValue().getUser());
    }

    @Test
    public void shouldExecuteOperationJobAndWrapJobOperationInChain() throws OperationException, InterruptedException, StoreException, SerialisationException {
        // Given
        final Operation operation = new GetVariables.Builder().variableNames(Lists.newArrayList()).build();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise("graphId", schema, properties);

        // When
        final JobDetail resultJobDetail = store.executeJob(operation, context);

        // Then
        Thread.sleep(1000);
        final ArgumentCaptor<JobDetail> jobDetail = ArgumentCaptor.forClass(JobDetail.class);
        verify(jobTracker, times(2)).addOrUpdateJob(jobDetail.capture(), eq(user));
        assertEquals(jobDetail.getAllValues().get(0), resultJobDetail);
        assertEquals(OperationChain.wrap(operation).toOverviewString(), resultJobDetail.getOpChain());
        assertEquals(JobStatus.FINISHED, jobDetail.getAllValues().get(1).getStatus());

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class), contextCaptor.capture(), eq(store));
        assertSame(user, contextCaptor.getValue().getUser());
    }

    @Test
    public void shouldExecuteOperationChainJobAndExportResults() throws OperationException, InterruptedException, StoreException {
        // Given
        final Operation operation = new GetVariables.Builder().variableNames(Lists.newArrayList()).build();
        final OperationChain<?> opChain = new OperationChain<>(operation);
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise("graphId", schema, properties);

        // When
        final JobDetail resultJobDetail = store.executeJob(opChain, context);

        // Then
        Thread.sleep(1000);
        final ArgumentCaptor<JobDetail> jobDetail = ArgumentCaptor.forClass(JobDetail.class);
        verify(jobTracker, times(2)).addOrUpdateJob(jobDetail.capture(), eq(user));
        assertEquals(jobDetail.getAllValues().get(0), resultJobDetail);
        assertEquals(JobStatus.FINISHED, jobDetail.getAllValues().get(1).getStatus());

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class), contextCaptor.capture(), eq(store));
        assertSame(user, contextCaptor.getValue().getUser());
    }

    @Test
    public void shouldGetJobTracker() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise("graphId", schema, properties);
        // When
        final JobTracker resultJobTracker = store.getJobTracker();

        // Then
        assertSame(jobTracker, resultJobTracker);
    }

    @Test
    public void shouldUpdateJsonSerialiser() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJsonSerialiserClass()).willReturn(TestCustomJsonSerialiser1.class.getName());
        given(properties.getJsonSerialiserModules()).willReturn(StorePropertiesTest.TestCustomJsonModules1.class.getName());
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        TestCustomJsonSerialiser1.mapper = mock(ObjectMapper.class);
        System.setProperty(JSONSerialiser.JSON_SERIALISER_CLASS_KEY, TestCustomJsonSerialiser1.class.getName());
        StorePropertiesTest.TestCustomJsonModules1.modules = asList(
                mock(Module.class),
                mock(Module.class)
        );

        final Store store = new StoreImpl();
        final Schema schema = new Schema();

        // When
        store.initialise("graphId", schema, properties);

        // Then
        assertEquals(TestCustomJsonSerialiser1.class, JSONSerialiser.getInstance().getClass());
        assertSame(TestCustomJsonSerialiser1.mapper, JSONSerialiser.getMapper());
        verify(TestCustomJsonSerialiser1.mapper, times(2)).registerModules(StorePropertiesTest.TestCustomJsonModules1.modules);
    }

    @Test
    public void shouldSetAndGetGraphLibrary() {
        // Given
        final Store store = new StoreImpl();
        final GraphLibrary graphLibrary = mock(GraphLibrary.class);

        // When
        store.setGraphLibrary(graphLibrary);
        final GraphLibrary result = store.getGraphLibrary();

        // Then
        assertSame(graphLibrary, result);
    }

    private Schema createSchemaMock() {
        final Schema schema = mock(Schema.class);
        given(schema.validate()).willReturn(new ValidationResult());
        given(schema.getVertexSerialiser()).willReturn(mock(Serialiser.class));
        return schema;
    }


    @Test
    public void shouldFindInvalidSerialiser() throws Exception {
        final Class<StringToStringSerialiser> invalidSerialiserClass = StringToStringSerialiser.class;
        Schema invalidSchema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("invalidString")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringSerialiser())
                        .build())
                .type("invalidString", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(invalidSerialiserClass.newInstance())
                        .build())
                .type("true", Boolean.class)
                .build();

        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        final Class<ToBytesSerialiser> validSerialiserInterface = ToBytesSerialiser.class;
        try {
            new StoreImpl() {
                @Override
                protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
                    return validSerialiserInterface;
                }
            }.initialise("graphId", invalidSchema, properties);
            fail("Should have thrown exception");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains(invalidSerialiserClass.getSimpleName()));
        }
    }

    @Test
    public void shouldCorrectlySetUpScheduledJobDetail() throws Exception {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        StoreImpl2 store = new StoreImpl2();

        store.initialise("graphId", schema, properties);

        final Repeat repeat = new Repeat(0, 100, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain
                .Builder()
                .first(new DiscardOutput())
                .build();
        final Context context = new Context(user);
        final String operationChainOverviewString = opChain.toOverviewString();
        final String serialisedOperationChain = new String(JSONSerialiser.serialise(opChain), Charset.forName(CommonConstants.UTF_8));

        // When - setup job
        JobDetail parentJobDetail = store.executeJob(new Job(repeat, opChain), context);

        ScheduledExecutorService service = store.getExecutorService();

        // Then - assert scheduled
        verify(service).scheduleAtFixedRate(
                any(Runnable.class),
                eq(repeat.getInitialDelay()),
                eq(repeat.getRepeatPeriod()),
                eq(repeat.getTimeUnit()));

        // Then - assert job detail is as expected
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());
        assertEquals(operationChainOverviewString, parentJobDetail.getOpChain());
        assertEquals(serialisedOperationChain, parentJobDetail.getSerialisedOperationChain());
        assertEquals(context.getUser(), parentJobDetail.getUser());
    }

    @Test
    public void shouldCorrectlyRescheduleJobsOnInitialisation() throws Exception {

        shouldRescheduleJobsCorrectlyWhenInitialisationCountIs(1);
    }

    @Test
    public void shouldOnlyRescheduleJobsOnceWhenInitialisationCalledMultipleTimes() throws Exception {

        shouldRescheduleJobsCorrectlyWhenInitialisationCountIs(5);
    }

    private void shouldRescheduleJobsCorrectlyWhenInitialisationCountIs(final int initialisationCount) throws Exception {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        final Repeat repeat = new Repeat(0, 100, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain
                .Builder()
                .first(new DiscardOutput())
                .build();

        final User user = new User.Builder()
                .userId("testUser")
                .opAuth("opAuth")
                .dataAuth("dataAuth")
                .build();

        final JobDetail scheduledJobDetail = new JobDetail.Builder()
                .jobId("jobId")
                .user(user)
                .opChain(opChain.toOverviewString())
                .serialisedOperationChain(opChain)
                .repeat(repeat)
                .build();

        given(jobTracker.getAllScheduledJobs()).willReturn(new WrappedCloseableIterable(singletonList(scheduledJobDetail)));

        StoreImpl2 store = new StoreImpl2();

        // When - initialise store
        for (int i = 0; i < initialisationCount; i++) {
            store.initialise("graphId", schema, properties);
        }

        ScheduledExecutorService service = store.getExecutorService();

        // Then - assert scheduled
        final ArgumentCaptor<ScheduledJobRunnable> scheduledJobRunnableCaptor = ArgumentCaptor.forClass(ScheduledJobRunnable.class);

        verify(service).scheduleAtFixedRate(
                scheduledJobRunnableCaptor.capture(),
                eq(repeat.getInitialDelay()),
                eq(repeat.getRepeatPeriod()),
                eq(repeat.getTimeUnit()));

        assertEquals(scheduledJobDetail, scheduledJobRunnableCaptor.getValue().getJobDetail());
        assertEquals(user, scheduledJobRunnableCaptor.getValue().getContext().getUser());
        assertArrayEquals(
                JSONSerialiser.serialise(opChain),
                JSONSerialiser.serialise(scheduledJobRunnableCaptor.getValue().getOperationChain()));
    }

    @Test
    public void shouldOptimiseOperationChains() throws Exception {
        // Given
        final StoreImpl store = new StoreImpl();
        store.initialise("graphId", createSchemaMock(), mock(StoreProperties.class));

        final CloseableIterable expectedResult = mock(CloseableIterable.class);

        // An input OperationChain
        final AddElements addElements = new AddElements();
        final GetElements getElements = new GetElements();
        final OperationChain<CloseableIterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(addElements)
                .then(getElements)
                .build();

        // The Operation contained in the optimised OperationChain
        final GetAllElements getAllElements = new GetAllElements();
        given(getAllElementsHandler.doOperation(getAllElements, context, store))
                .willReturn(expectedResult);

        // Create OperationChain optimiser
        store.addOperationChainOptimisers(asList(new TestOperationChainOptimiser(asList(getAllElements))));

        // When
        final CloseableIterable<? extends Element> result = store.execute(opChain, context);

        // Then
        assertSame(expectedResult, result);
        verify(getAllElementsHandler).doOperation(getAllElements, context, store);
        verify(addElementsHandler, never()).doOperation(addElements, context, store);
        verify(getElementsHandler, never()).doOperation(getElements, context, store);
    }

    private class TestOperationChainOptimiser extends AbstractOperationChainOptimiser {

        private final List<Operation> optimisedOperationList;

        TestOperationChainOptimiser(final List<Operation> optimisedOperationList) {
            this.optimisedOperationList = optimisedOperationList;
        }

        @Override
        protected List<Operation> addPreOperations(final Operation previousOp, final Operation currentOp) {
            return emptyList();
        }

        @Override
        protected List<Operation> optimiseCurrentOperation(final Operation previousOp, final Operation currentOp, final Operation nextOp) {
            return emptyList();
        }

        @Override
        protected List<Operation> addPostOperations(final Operation currentOp, final Operation nextOp) {
            return emptyList();
        }

        @Override
        protected List<Operation> optimiseAll(final List<Operation> ops) {
            return optimisedOperationList;
        }
    }


    private class StoreImpl extends Store {
        private final Set<StoreTrait> traits = new HashSet<>(asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, TRANSFORMATION, ORDERED));
        private final ArrayList<Operation> doUnhandledOperationCalls = new ArrayList<>();
        private int createOperationHandlersCallCount;
        private final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);

        @Override
        protected OperationChainValidator createOperationChainValidator() {
            return operationChainValidator;
        }

        @Override
        public Set<StoreTrait> getTraits() {
            return traits;
        }

        public OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
            return super.getOperationHandler(opClass);
        }

        @Override
        public OperationHandler<Operation> getOperationHandler(final Class<? extends Operation> opClass) {
            if (opClass.equals(SetVariable.class)) {
                return null;
            }
            return super.getOperationHandler(opClass);
        }

        @Override
        protected void addAdditionalOperationHandlers() {
            createOperationHandlersCallCount++;
            addOperationHandler(mock(AddElements.class).getClass(), addElementsHandler);
            addOperationHandler(mock(GetElements.class).getClass(), (OperationHandler) getElementsHandler);
            addOperationHandler(mock(GetAdjacentIds.class).getClass(), getElementsHandler);
            addOperationHandler(Validate.class, validateHandler);
            addOperationHandler(ExportToGafferResultCache.class, exportToGafferResultCacheHandler);
            addOperationHandler(GetGafferResultCacheExport.class, getGafferResultCacheExportHandler);
        }

        @Override
        protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
            return getElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
            return getAllElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
            return getAdjacentIdsHandler;
        }

        @Override
        protected OperationHandler<AddElements> getAddElementsHandler() {
            return addElementsHandler;
        }

        @Override
        protected Object doUnhandledOperation(final Operation operation, final Context context) {
            doUnhandledOperationCalls.add(operation);
            return null;
        }

        public int getCreateOperationHandlersCallCount() {
            return createOperationHandlersCallCount;
        }

        public ArrayList<Operation> getDoUnhandledOperationCalls() {
            return doUnhandledOperationCalls;
        }

        @Override
        public void optimiseSchema() {
            schemaOptimiser.optimise(getSchema(), hasTrait(StoreTrait.ORDERED));
        }

        @Override
        protected JobTracker createJobTracker() {
            if (getProperties().getJobTrackerEnabled()) {
                return jobTracker;
            }

            return null;
        }

        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return Serialiser.class;
        }
    }

    // Second store implementation with overriding ExecutorService.
    // This cannot be done in the first because the other tests for Jobs will fail due to mocking.
    private class StoreImpl2 extends Store {
        private final Set<StoreTrait> traits = new HashSet<>(asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, TRANSFORMATION, ORDERED));
        private final ArrayList<Operation> doUnhandledOperationCalls = new ArrayList<>();
        private int createOperationHandlersCallCount;
        private final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);

        @Override
        protected OperationChainValidator createOperationChainValidator() {
            return operationChainValidator;
        }

        @Override
        public Set<StoreTrait> getTraits() {
            return traits;
        }

        public OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
            return super.getOperationHandler(opClass);
        }

        @Override
        public OperationHandler<Operation> getOperationHandler(final Class<? extends Operation> opClass) {
            if (opClass.equals(SetVariable.class)) {
                return null;
            }
            return super.getOperationHandler(opClass);
        }

        @Override
        protected void addAdditionalOperationHandlers() {
            createOperationHandlersCallCount++;
            addOperationHandler(mock(AddElements.class).getClass(), addElementsHandler);
            addOperationHandler(mock(GetElements.class).getClass(), (OperationHandler) getElementsHandler);
            addOperationHandler(mock(GetAdjacentIds.class).getClass(), getElementsHandler);
            addOperationHandler(Validate.class, validateHandler);
            addOperationHandler(ExportToGafferResultCache.class, exportToGafferResultCacheHandler);
            addOperationHandler(GetGafferResultCacheExport.class, getGafferResultCacheExportHandler);
        }

        @Override
        protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
            return getElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
            return getAllElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
            return getAdjacentIdsHandler;
        }

        @Override
        protected OperationHandler<AddElements> getAddElementsHandler() {
            return addElementsHandler;
        }

        @Override
        protected Object doUnhandledOperation(final Operation operation, final Context context) {
            doUnhandledOperationCalls.add(operation);
            return null;
        }

        public int getCreateOperationHandlersCallCount() {
            return createOperationHandlersCallCount;
        }

        public ArrayList<Operation> getDoUnhandledOperationCalls() {
            return doUnhandledOperationCalls;
        }

        @Override
        public void optimiseSchema() {
            schemaOptimiser.optimise(getSchema(), hasTrait(StoreTrait.ORDERED));
        }

        @Override
        protected JobTracker createJobTracker() {
            if (getProperties().getJobTrackerEnabled()) {
                return jobTracker;
            }

            return null;
        }

        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return Serialiser.class;
        }

        @Override
        protected ScheduledExecutorService getExecutorService() {
            ScheduledFuture<?> future = Mockito.mock(RunnableScheduledFuture.class);
            doReturn(future).when(executorService).scheduleAtFixedRate(
                    any(Runnable.class),
                    anyLong(),
                    anyLong(),
                    any(TimeUnit.class)
            );
            return executorService;
        }
    }

    public static final class TestCustomJsonSerialiser1 extends JSONSerialiser {
        public static ObjectMapper mapper;

        public TestCustomJsonSerialiser1() {
            super(mapper);
        }
    }
}
