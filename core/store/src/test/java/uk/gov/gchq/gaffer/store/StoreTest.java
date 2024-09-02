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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
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
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
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
import uk.gov.gchq.gaffer.operation.impl.get.GetGraphCreatedTime;
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
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.HasTrait;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetTraitsHandler;
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

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.jobtracker.JobTracker.JOB_TRACKER_CACHE_SERVICE_NAME;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

@ExtendWith(MockitoExtension.class)
public class StoreTest {
    private final User user = new User("user01");
    private final Context context = new Context(user);

    private Schema schema;
    private StoreImpl store;

    @Mock
    private OperationHandler<AddElements> addElementsHandler;
    @Mock
    private OutputOperationHandler<GetElements, Iterable<? extends Element>> getElementsHandler;
    @Mock
    private OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getAllElementsHandler;
    @Mock
    private OutputOperationHandler<GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler;
    @Mock
    private OperationHandler<Validate> validateHandler;
    @Mock
    private SchemaOptimiser schemaOptimiser;
    @Mock
    private JobTracker jobTracker;
    @Mock
    private OperationHandler<ExportToGafferResultCache<?>> exportToGafferResultCacheHandler;
    @Mock
    private OperationHandler<GetGafferResultCacheExport> getGafferResultCacheExportHandler;
    @Mock
    private OperationChainValidator operationChainValidator;

    @BeforeEach
    public void setup() {
        CacheServiceLoader.shutdown();
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_CLASS_KEY);
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_MODULES);
        JSONSerialiser.update();

        store = new StoreImpl();
        lenient().when(schemaOptimiser.optimise(any(Schema.class), any(Boolean.class))).then(returnsFirstArg());
        lenient().when(operationChainValidator.validate(any(OperationChain.class), any(User.class), any(Store.class)))
                .thenReturn(new ValidationResult());

        schema = new Schema.Builder().edge(TestGroups.EDGE,
                new SchemaEdgeDefinition.Builder().source("string")
                        .destination("string")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .edge(TestGroups.EDGE_2,
                        new SchemaEdgeDefinition.Builder().source("string")
                                .destination("string")
                                .directed("true")
                                .property(TestPropertyNames.PROP_1, "string")
                                .property(TestPropertyNames.PROP_2, "string")
                                .build())
                .entity(TestGroups.ENTITY,
                        new SchemaEntityDefinition.Builder().vertex("string")
                                .property(TestPropertyNames.PROP_1, "string")
                                .property(TestPropertyNames.PROP_2, "string")
                                .build())
                .entity(TestGroups.ENTITY_2,
                        new SchemaEntityDefinition.Builder().vertex("string")
                                .property(TestPropertyNames.PROP_1, "string")
                                .property(TestPropertyNames.PROP_2, "string")
                                .build())
                .type("string", new TypeDefinition.Builder().clazz(String.class)
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
    public void shouldExecuteOperationWhenJobTrackerCacheIsBroken(@Mock final StoreProperties storeProperties) throws Exception {
        // Given
        ICache<Object, Object> mockICache = Mockito.mock(ICache.class);
        doThrow(new CacheOperationException("Stubbed class")).when(mockICache).put(any(), any());
        ICacheService mockICacheService = Mockito.spy(ICacheService.class);
        given(mockICacheService.getCache(any())).willReturn(mockICache);

        Field field = CacheServiceLoader.class.getDeclaredField("SERVICES");
        field.setAccessible(true);
        java.util.Map<String, ICacheService> mockCacheServices = (java.util.Map<String, ICacheService>) field.get(new HashMap<>());
        mockCacheServices.put(JOB_TRACKER_CACHE_SERVICE_NAME, mockICacheService);

        final AddElements addElements = new AddElements();
        final StoreImpl3 store = new StoreImpl3();
        store.initialise("graphId", createSchemaMock(), storeProperties);

        // When
        store.execute(addElements, context);

        // Then
        verify(addElementsHandler).doOperation(addElements, context, store);
        verify(mockICacheService, Mockito.atLeast(1)).getCache(any());
        verify(mockICache, Mockito.atLeast(1)).put(any(), any());
    }

    @Test
    public void shouldCreateStoreWithSpecificCaches() throws SchemaException, StoreException {
        // Given
        final Store testStore = new StoreImpl();

        // When
        testStore.initialise("testGraph", new Schema(), StoreProperties.loadStoreProperties("allCaches.properties"));

        // Then
        assertThat(CacheServiceLoader.isDefaultEnabled()).isFalse();
        assertThat(CacheServiceLoader.isEnabled("JobTracker")).isTrue();
        assertThat(CacheServiceLoader.isEnabled("NamedView")).isTrue();
        assertThat(CacheServiceLoader.isEnabled("NamedOperation")).isTrue();
    }

    @Test
    public void shouldCreateStoreWithDefaultCache() throws SchemaException, StoreException {
        // Given
        final Store testStore = new StoreImpl();
        final StoreProperties props = new StoreProperties();

        // When
        props.setDefaultCacheServiceClass(HashMapCacheService.class.getName());
        testStore.initialise("testGraph", new Schema(), props);

        // Then
        assertThat(CacheServiceLoader.isDefaultEnabled()).isTrue();
    }

    @Test
    public void shouldThrowExceptionIfGraphIdIsNull(@Mock final StoreProperties properties) throws Exception {
        assertThatIllegalArgumentException().isThrownBy(() -> store.initialise(null, schema, properties))
                .extracting("message").isNotNull();
    }

    @Test
    public void shouldThrowExceptionWhenPropertyIsNotSerialisable(@Mock final StoreProperties properties) throws StoreException {
        // Given
        final Schema mySchema =
                new Schema.Builder().edge(TestGroups.EDGE,
                        new SchemaEdgeDefinition.Builder().property(TestPropertyNames.PROP_1, "invalidType").build())
                        .type("invalidType", new TypeDefinition.Builder().clazz(Object.class)
                                .serialiser(new StringSerialiser())
                                .build())
                        .build();

        // When
        assertThatExceptionOfType(SchemaException.class).isThrownBy(() -> store.initialise("graphId", mySchema, properties))
                .extracting("message").isNotNull();
    }

    @Test
    public void shouldCreateStoreWithValidSchemasAndRegisterOperations(@Mock final StoreProperties properties,
                                                                       @Mock final OperationHandler<AddElements> addElementsHandlerOverridden)
            throws StoreException {
        // Given
        final OperationDeclarations opDeclarations =
                new OperationDeclarations.Builder().declaration(new OperationDeclaration.Builder().operation(AddElements.class)
                        .handler(addElementsHandlerOverridden)
                        .build())
                        .build();
        given(properties.getOperationDeclarations()).willReturn(opDeclarations);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        // When
        store.initialise("graphId", schema, properties);

        // Then
        assertThat(store.getOperationHandlerExposed(Validate.class)).isNotNull();
        assertThat(store.getOperationHandlerExposed(AddElements.class)).isSameAs(addElementsHandlerOverridden);

        assertThat(store.getOperationHandlerExposed(GetAllElements.class)).isSameAs(getAllElementsHandler);

        assertThat(store.getOperationHandlerExposed(GenerateElements.class)).isInstanceOf(GenerateElementsHandler.class);
        assertThat(store.getOperationHandlerExposed(GenerateObjects.class)).isInstanceOf(GenerateObjectsHandler.class);

        assertThat(store.getOperationHandlerExposed(CountGroups.class)).isInstanceOf(CountGroupsHandler.class);
        assertThat(store.getOperationHandlerExposed(ToSet.class)).isInstanceOf(ToSetHandler.class);

        assertThat(store.getOperationHandlerExposed(ExportToSet.class)).isInstanceOf(ExportToSetHandler.class);
        assertThat(store.getOperationHandlerExposed(GetSetExport.class)).isInstanceOf(GetSetExportHandler.class);

        assertThat(store.getCreateOperationHandlersCallCount()).isEqualTo(1);
        assertThat(store.getSchema()).isSameAs(schema);
        assertThat(store.getProperties()).isSameAs(properties);

        verify(schemaOptimiser).optimise(store.getSchema(), true);
    }

    @Test
    public void shouldDelegateDoOperationToOperationHandler(@Mock final StoreProperties properties) throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final AddElements addElements = new AddElements();
        store.initialise("graphId", schema, properties);

        // When
        store.execute(addElements, context);

        // Then
        verify(addElementsHandler).doOperation(addElements, context, store);
    }

    @Test
    public void shouldCloseOperationIfResultIsNotCloseable(@Mock final StoreProperties properties,
                                                           @Mock final Operation operation)
            throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final StoreImpl store = new StoreImpl();
        store.initialise("graphId", schema, properties);

        // When
        store.handleOperation(operation, context);

        // Then
        verify(operation).close();
    }

    @Test
    public void shouldCloseOperationIfExceptionThrown(@Mock final StoreProperties properties,
                                                      @Mock final OperationHandler<?> opHandler,
                                                      @Mock final Operation operation)
            throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final StoreImpl store = new StoreImpl();
        store.addOperationHandler(Operation.class, opHandler);
        store.initialise("graphId", schema, properties);

        // When / Then
        try {
            store.handleOperation(operation, context);
        } catch (final Exception e) {
            verify(operation).close();
        }
    }

    @Test
    public void shouldThrowExceptionIfOperationChainIsInvalid(@Mock final StoreProperties properties) throws OperationException, StoreException {
        // Given
        final Schema schema = createSchemaMock();
        final OperationChain<?> opChain = new OperationChain<>();
        final StoreImpl store = new StoreImpl();

        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(schema.validate()).willReturn(new ValidationResult());
        final ValidationResult validationResult = new ValidationResult();
        validationResult.addError("error");
        given(operationChainValidator.validate(opChain, user, store)).willReturn(validationResult);
        store.initialise("graphId", schema, properties);

        // When / Then
        assertThatIllegalArgumentException().isThrownBy(() -> store.execute(opChain, context))
                .withMessageContaining("Operation chain");
        verify(operationChainValidator).validate(opChain, user, store);
    }

    @Test
    public void shouldCallDoUnhandledOperationWhenDoOperationWithUnknownOperationClass(@Mock final StoreProperties properties) throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final Operation operation = new SetVariable.Builder().variableName("aVariable").input("inputString").build();
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        store.initialise("graphId", schema, properties);

        // When
        store.execute(operation, context);

        // Then
        assertThat(store.getDoUnhandledOperationCalls()).hasSize(1);
        assertThat(store.getDoUnhandledOperationCalls().get(0)).isSameAs(operation);
    }

    @Test
    public void shouldFullyLoadLazyElement(@Mock final StoreProperties properties,
                                           @Mock final LazyEntity lazyElement,
                                           @Mock final Entity entity)
            throws StoreException {
        // Given
        final Store store = new StoreImpl();
        given(lazyElement.getGroup()).willReturn(TestGroups.ENTITY);
        given(lazyElement.getElement()).willReturn(entity);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        store.initialise("graphId", schema, properties);

        // When
        final Element result = store.populateElement(lazyElement);

        // Then
        assertThat(result).isSameAs(entity);
        verify(lazyElement).getGroup();
        verify(lazyElement).getProperty(TestPropertyNames.PROP_1);
        verify(lazyElement).getIdentifier(IdentifierType.VERTEX);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldHandleMultiStepOperations(@Mock final StoreProperties properties,
                                                @Mock final Iterable getElementsResult)
            throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        final AddElements addElements1 = new AddElements();
        final GetElements getElements = new GetElements();
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(addElements1)
                .then(getElements)
                .build();

        given(addElementsHandler.doOperation(addElements1, context, store)).willReturn(null);
        given(getElementsHandler.doOperation(getElements, context, store)).willReturn(getElementsResult);

        store.initialise("graphId", schema, properties);

        // When
        final Iterable<? extends Element> result = store.execute(opChain, context);

        // Then
        assertThat(result).isSameAs(getElementsResult);
    }

    @Test
    public void shouldReturnAllSupportedOperationsWhenJobTrackerIsEnabled(@Mock final StoreProperties properties) throws Exception {
        // Given
        CacheServiceLoader.initialise(HashMapCacheService.class.getName());

        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        given(properties.getNamedViewEnabled()).willReturn(true);
        given(properties.getNamedOperationEnabled()).willReturn(true);
        store.initialise("graphId", schema, properties);

        // When
        final List<Class<? extends Operation>> supportedOperations = Lists.newArrayList(store.getSupportedOperations());

        // Then
        assertThat(supportedOperations).isNotNull();

        final List<Class<? extends Operation>> expectedOperations =
                Lists.newArrayList(AddElements.class,
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
                        HasTrait.class,
                        While.class,
                        Join.class,
                        ToSingletonList.class,
                        ForEach.class,
                        Reduce.class,
                        CancelScheduledJob.class,
                        GetGraphCreatedTime.class,

                        // Function
                        Filter.class,
                        Transform.class,
                        Aggregate.class,

                        // Context variables
                        SetVariable.class,
                        GetVariable.class,
                        GetVariables.class);

        expectedOperations.sort(Comparator.comparing(Class::getName));
        supportedOperations.sort(Comparator.comparing(Class::getName));
        assertThat(supportedOperations).isEqualTo(expectedOperations);
    }

    @Test
    public void shouldReturnAllSupportedOperationsWhenJobTrackerIsDisabled(@Mock final StoreProperties properties) throws Exception {
        // Given
        CacheServiceLoader.initialise(HashMapCacheService.class.getName());

        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(false);
        given(properties.getNamedViewEnabled()).willReturn(true);
        given(properties.getNamedOperationEnabled()).willReturn(true);
        store.initialise("graphId", schema, properties);

        // When
        final List<Class<? extends Operation>> supportedOperations = Lists.newArrayList(store.getSupportedOperations());

        // Then

        assertThat(supportedOperations).isNotNull();

        final List<Class<? extends Operation>> expectedOperations =
                Lists.newArrayList(AddElements.class,
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
                        HasTrait.class,
                        Map.class,
                        If.class,
                        While.class,
                        Join.class,
                        ToSingletonList.class,
                        ForEach.class,
                        Reduce.class,
                        GetGraphCreatedTime.class,

                        // Function
                        Filter.class,
                        Transform.class,
                        Aggregate.class,

                        // Context variables
                        SetVariable.class,
                        GetVariable.class,
                        GetVariables.class);

        expectedOperations.sort(Comparator.comparing(Class::getName));
        supportedOperations.sort(Comparator.comparing(Class::getName));
        assertThat(supportedOperations).isEqualTo(expectedOperations);
    }

    @Test
    public void shouldReturnTrueWhenOperationSupported(@Mock final StoreProperties properties) throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise("graphId", schema, properties);

        // WHen
        final Set<Class<? extends Operation>> supportedOperations = store.getSupportedOperations();
        for (final Class<? extends Operation> operationClass : supportedOperations) {
            final boolean isOperationClassSupported = store.isSupported(operationClass);

            // Then
            assertThat(isOperationClassSupported).isTrue();
        }
    }

    @Test
    public void shouldReturnFalseWhenUnsupportedOperationRequested(@Mock final StoreProperties properties) throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise("graphId", schema, properties);

        // When
        final boolean supported = store.isSupported(Operation.class);

        // Then
        assertThat(supported).isFalse();
    }

    @Test
    public void shouldHandleNullOperationSupportRequest(@Mock final StoreProperties properties) throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise("graphId", schema, properties);

        // When
        final boolean supported = store.isSupported(null);

        // Then
        assertThat(supported).isFalse();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldExecuteOperationChainJob(@Mock final StoreProperties properties) throws OperationException, InterruptedException, StoreException {
        // Given
        final Operation operation = new GetVariables.Builder().variableNames(Lists.newArrayList()).build();
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(operation)
                .then(new ExportToGafferResultCache())
                .build();
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
        assertThat(resultJobDetail).isEqualTo(jobDetail.getAllValues().get(0));
        assertThat(jobDetail.getAllValues().get(1).getStatus()).isEqualTo(JobStatus.FINISHED);

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class),
                contextCaptor.capture(), eq(store));
        assertThat(contextCaptor.getValue().getUser()).isSameAs(user);
    }

    @Test
    public void shouldExecuteOperationJobAndWrapJobOperationInChain(@Mock final StoreProperties properties)
            throws OperationException, InterruptedException, StoreException, SerialisationException {
        // Given
        final Operation operation = new GetVariables.Builder().variableNames(Lists.newArrayList()).build();
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

        assertThat(resultJobDetail).isEqualTo(jobDetail.getAllValues().get(0));
        assertThat(resultJobDetail.getOpChain()).isEqualTo(OperationChain.wrap(operation).toOverviewString());
        assertThat(jobDetail.getAllValues().get(1).getStatus()).isEqualTo(JobStatus.FINISHED);

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class),
                contextCaptor.capture(), eq(store));
        assertThat(contextCaptor.getValue().getUser()).isSameAs(user);
    }

    @Test
    public void shouldExecuteOperationChainJobAndExportResults(@Mock final StoreProperties properties)
            throws OperationException, InterruptedException, StoreException {
        // Given
        final Operation operation = new GetVariables.Builder().variableNames(Lists.newArrayList()).build();
        final OperationChain<?> opChain = new OperationChain<>(operation);
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

        assertThat(resultJobDetail).isEqualTo(jobDetail.getAllValues().get(0));
        assertThat(jobDetail.getAllValues().get(1).getStatus()).isEqualTo(JobStatus.FINISHED);

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class),
                contextCaptor.capture(), eq(store));
        assertThat(contextCaptor.getValue().getUser()).isSameAs(user);
    }

    @Test
    public void shouldGetJobTracker(@Mock final StoreProperties properties) throws StoreException {
        // Given
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise("graphId", schema, properties);
        // When
        final JobTracker resultJobTracker = store.getJobTracker();

        // Then
        assertThat(resultJobTracker).isSameAs(jobTracker);
    }

    @Test
    public void shouldUpdateJsonSerialiser(@Mock final StoreProperties properties,
                                           @Mock final ObjectMapper mockObjectMapper)
            throws StoreException {
        // Given
        given(properties.getJsonSerialiserClass()).willReturn(TestCustomJsonSerialiser1.class.getName());
        given(properties.getJsonSerialiserModules()).willReturn(StorePropertiesTest.TestCustomJsonModules1.class.getName());
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        TestCustomJsonSerialiser1.mapper = mockObjectMapper;
        System.setProperty(JSONSerialiser.JSON_SERIALISER_CLASS_KEY, TestCustomJsonSerialiser1.class.getName());
        StorePropertiesTest.TestCustomJsonModules1.modules = asList(
                mock(Module.class),
                mock(Module.class));

        final Store store = new StoreImpl();
        final Schema schema = new Schema();

        // When
        store.initialise("graphId", schema, properties);

        // Then
        assertThat(JSONSerialiser.getInstance()).isInstanceOf(TestCustomJsonSerialiser1.class);
        assertThat(JSONSerialiser.getMapper()).isSameAs(TestCustomJsonSerialiser1.mapper);
        verify(TestCustomJsonSerialiser1.mapper, times(2)).registerModules(StorePropertiesTest.TestCustomJsonModules1.modules);
    }

    @Test
    public void shouldSetAndGetGraphLibrary(@Mock final GraphLibrary graphLibrary) {
        // Given
        final Store store = new StoreImpl();

        // When
        store.setGraphLibrary(graphLibrary);
        final GraphLibrary result = store.getGraphLibrary();

        // Then
        assertThat(result).isSameAs(graphLibrary);
    }

    @Test
    void shouldGetCreatedTime() {
        // Given
        final Store testStore = new StoreImpl();

        // When
        String storeTime = testStore.getCreatedTime();

        // Then
        assertThat(storeTime).isInstanceOf(String.class);
        assertThat(LocalDateTime.parse(storeTime)).isInstanceOf(LocalDateTime.class);
    }

    private Schema createSchemaMock() {
        final Schema schema = mock(Schema.class);
        given(schema.validate()).willReturn(new ValidationResult());
        given(schema.getVertexSerialiser()).willReturn(mock(Serialiser.class));
        return schema;
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void shouldFindInvalidSerialiser(@Mock final StoreProperties properties) throws Exception {
        final Class<StringToStringSerialiser> invalidSerialiserClass = StringToStringSerialiser.class;
        final Schema invalidSchema =
                new Schema.Builder().edge(TestGroups.EDGE,
                        new SchemaEdgeDefinition.Builder().source("string")
                                .destination("invalidString")
                                .directed("true")
                                .property(TestPropertyNames.PROP_1, "string")
                                .property(TestPropertyNames.PROP_2, "string")
                                .build())
                        .type("string", new TypeDefinition.Builder().clazz(String.class)
                                .serialiser(new StringSerialiser())
                                .build())
                        .type("invalidString", new TypeDefinition.Builder().clazz(String.class)
                                .serialiser(invalidSerialiserClass.newInstance())
                                .build())
                        .type("true", Boolean.class)
                        .build();

        final Class<ToBytesSerialiser> validSerialiserInterface = ToBytesSerialiser.class;
        assertThatExceptionOfType(SchemaException.class).isThrownBy(() -> {
            new StoreImpl() {
                @Override
                protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
                    return validSerialiserInterface;
                }
            }.initialise("graphId", invalidSchema, properties);
        }).withMessageContaining(invalidSerialiserClass.getSimpleName());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void shouldCorrectlySetUpScheduledJobDetail(@Mock final StoreProperties properties) throws Exception {
        // Given
        given(properties.getJobTrackerEnabled()).willReturn(true);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        final StoreImpl2 store = new StoreImpl2();

        store.initialise("graphId", schema, properties);

        final Repeat repeat = new Repeat(0, 100, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder().first(new DiscardOutput()).build();
        final Context context = new Context(user);
        final String operationChainOverviewString = opChain.toOverviewString();
        final String serialisedOperationChain = new String(JSONSerialiser.serialise(opChain), StandardCharsets.UTF_8);

        // When - setup job
        final JobDetail parentJobDetail = store.executeJob(new Job(repeat, opChain), context);

        final ScheduledExecutorService service = store.getExecutorService();

        // Then - assert scheduled
        verify(service).scheduleAtFixedRate(any(Runnable.class),
                eq(repeat.getInitialDelay()),
                eq(repeat.getRepeatPeriod()),
                eq(repeat.getTimeUnit()));

        // Then - assert job detail is as expected
        assertThat(parentJobDetail.getStatus()).isEqualTo(JobStatus.SCHEDULED_PARENT);
        assertThat(parentJobDetail.getOpChain()).isEqualTo(operationChainOverviewString);
        assertThat(parentJobDetail.getSerialisedOperationChain()).isEqualTo(serialisedOperationChain);
        assertThat(parentJobDetail.getUser()).isEqualTo(context.getUser());
    }

    @Test
    public void shouldCorrectlyRescheduleJobsOnInitialisation() throws Exception {
        shouldRescheduleJobsCorrectlyWhenInitialisationCountIs(1);
    }

    @Test
    public void shouldOnlyRescheduleJobsOnceWhenInitialisationCalledMultipleTimes() throws Exception {
        shouldRescheduleJobsCorrectlyWhenInitialisationCountIs(5);
    }

    private void shouldRescheduleJobsCorrectlyWhenInitialisationCountIs(final int initialisationCount)
            throws Exception {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getRescheduleJobsOnStart()).willReturn(true);

        final Repeat repeat = new Repeat(0, 100, TimeUnit.SECONDS);
        final OperationChain<?> opChain = new OperationChain.Builder().first(new DiscardOutput()).build();

        final User user = new User.Builder().userId("testUser")
                .opAuth("opAuth")
                .dataAuth("dataAuth")
                .build();

        final JobDetail scheduledJobDetail = new JobDetail.Builder().jobId("jobId")
                .user(user)
                .opChain(opChain.toOverviewString())
                .serialisedOperationChain(opChain)
                .repeat(repeat)
                .build();

        given(jobTracker.getAllScheduledJobs()).willReturn(singletonList(scheduledJobDetail));

        final StoreImpl2 store = new StoreImpl2();

        // When - initialise store
        for (int i = 0; i < initialisationCount; i++) {
            store.initialise("graphId", schema, properties);
        }

        final ScheduledExecutorService service = store.getExecutorService();

        // Then - assert scheduled
        final ArgumentCaptor<ScheduledJobRunnable> scheduledJobRunnableCaptor = ArgumentCaptor.forClass(ScheduledJobRunnable.class);

        verify(service).scheduleAtFixedRate(scheduledJobRunnableCaptor.capture(),
                eq(repeat.getInitialDelay()),
                eq(repeat.getRepeatPeriod()),
                eq(repeat.getTimeUnit()));

        assertThat(scheduledJobRunnableCaptor.getValue().getJobDetail()).isEqualTo(scheduledJobDetail);
        assertThat(scheduledJobRunnableCaptor.getValue().getContext().getUser()).isEqualTo(user);
        assertThat(JSONSerialiser.serialise(scheduledJobRunnableCaptor.getValue().getOperationChain()))
                .isEqualTo(JSONSerialiser.serialise(opChain));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldOptimiseOperationChains(@Mock final StoreProperties properties,
                                              @Mock final Iterable expectedResult)
            throws Exception {
        // Given
        final StoreImpl store = new StoreImpl();
        store.initialise("graphId", createSchemaMock(), properties);

        // An input OperationChain
        final AddElements addElements = new AddElements();
        final GetElements getElements = new GetElements();
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder().first(addElements)
                .then(getElements)
                .build();

        // The Operation contained in the optimised OperationChain
        final GetAllElements getAllElements = new GetAllElements();
        given(getAllElementsHandler.doOperation(getAllElements, context, store)).willReturn(expectedResult);

        // Create OperationChain optimiser
        store.addOperationChainOptimisers(asList(new TestOperationChainOptimiser(asList(getAllElements))));

        // When
        final Iterable<? extends Element> result = store.execute(opChain, context);

        // Then
        assertThat(result).isSameAs(expectedResult);
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
        protected List<Operation> optimiseCurrentOperation(final Operation previousOp, final Operation currentOp,
                                                           final Operation nextOp) {
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
        private final Set<StoreTrait> traits =
                new HashSet<>(asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, TRANSFORMATION, ORDERED));
        private final ArrayList<Operation> doUnhandledOperationCalls = new ArrayList<>();
        private int createOperationHandlersCallCount;

        @Mock
        private ScheduledExecutorService executorService;

        @Override
        protected OperationChainValidator createOperationChainValidator() {
            return operationChainValidator;
        }

        @SuppressWarnings("rawtypes")
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
            addOperationHandler(mock(GetElements.class).getClass(), getElementsHandler);
            addOperationHandler(mock(GetAdjacentIds.class).getClass(), getElementsHandler);
            addOperationHandler(Validate.class, validateHandler);
            addOperationHandler(ExportToGafferResultCache.class, exportToGafferResultCacheHandler);
            addOperationHandler(GetGafferResultCacheExport.class, getGafferResultCacheExportHandler);
        }

        @Override
        protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
            return getElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
            return getAllElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
            return getAdjacentIdsHandler;
        }

        @Override
        protected OperationHandler<AddElements> getAddElementsHandler() {
            return addElementsHandler;
        }


        @Override
        protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
            return new GetTraitsHandler(traits);
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
        protected SchemaOptimiser createSchemaOptimiser() {
            return schemaOptimiser;
        }

        @Override
        protected void populateCaches() {
            if (getProperties().getJobTrackerEnabled()) {
                super.jobTracker = StoreTest.this.jobTracker;
            }
        }

        @SuppressWarnings("rawtypes")
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

        private final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);

        @Override
        protected OperationChainValidator createOperationChainValidator() {
            return operationChainValidator;
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
            addOperationHandler(mock(AddElements.class).getClass(), addElementsHandler);
            addOperationHandler(mock(GetElements.class).getClass(), getElementsHandler);
            addOperationHandler(mock(GetAdjacentIds.class).getClass(), getElementsHandler);
            addOperationHandler(Validate.class, validateHandler);
            addOperationHandler(ExportToGafferResultCache.class, exportToGafferResultCacheHandler);
            addOperationHandler(GetGafferResultCacheExport.class, getGafferResultCacheExportHandler);
        }

        @Override
        protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
            return getElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
            return getAllElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
            return getAdjacentIdsHandler;
        }

        @Override
        protected OperationHandler<AddElements> getAddElementsHandler() {
            return addElementsHandler;
        }


        @Override
        protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
            return new GetTraitsHandler(traits);
        }

        @Override
        protected Object doUnhandledOperation(final Operation operation, final Context context) {
            doUnhandledOperationCalls.add(operation);
            return null;
        }

        @Override
        protected void populateCaches() {
            if (getProperties().getJobTrackerEnabled()) {
                super.jobTracker = StoreTest.this.jobTracker;
            }
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return Serialiser.class;
        }

        @Override
        protected ScheduledExecutorService getExecutorService() {
            final ScheduledFuture<?> future = mock(RunnableScheduledFuture.class);
            lenient().doReturn(future).when(executorService).scheduleAtFixedRate(any(Runnable.class),
                    anyLong(),
                    anyLong(),
                    any(TimeUnit.class));
            return executorService;
        }
    }

    public static final class TestCustomJsonSerialiser1 extends JSONSerialiser {
        public static ObjectMapper mapper;

        public TestCustomJsonSerialiser1() {
            super(mapper);
        }
    }
    private class StoreImpl3 extends Store {
        private final Set<StoreTrait> traits =
                new HashSet<>(asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, TRANSFORMATION, ORDERED));
        private final ArrayList<Operation> doUnhandledOperationCalls = new ArrayList<>();
        private int createOperationHandlersCallCount;

        @Mock
        private ScheduledExecutorService executorService;

        @Override
        protected OperationChainValidator createOperationChainValidator() {
            return operationChainValidator;
        }

        @SuppressWarnings("rawtypes")
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
            addOperationHandler(mock(GetElements.class).getClass(), getElementsHandler);
            addOperationHandler(mock(GetAdjacentIds.class).getClass(), getElementsHandler);
            addOperationHandler(Validate.class, validateHandler);
            addOperationHandler(ExportToGafferResultCache.class, exportToGafferResultCacheHandler);
            addOperationHandler(GetGafferResultCacheExport.class, getGafferResultCacheExportHandler);
        }

        @Override
        protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
            return getElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
            return getAllElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
            return getAdjacentIdsHandler;
        }

        @Override
        protected OperationHandler<AddElements> getAddElementsHandler() {
            return addElementsHandler;
        }

        @Override
        protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
            return new GetTraitsHandler(traits);
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
        protected SchemaOptimiser createSchemaOptimiser() {
            return schemaOptimiser;
        }

        @Override
        protected void populateCaches() {
            super.jobTracker = new JobTracker("Test");
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return Serialiser.class;
        }
    }
}
