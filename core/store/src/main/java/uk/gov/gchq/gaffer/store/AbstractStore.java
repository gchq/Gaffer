/*
 * Copyright 2016-2018 Crown Copyright
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.OperationUtil;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A {@code Store} backs a Graph and is responsible for storing the {@link
 * uk.gov.gchq.gaffer.data.element.Element}s and
 * handling {@link Operation}s.
 * {@link Operation}s and their corresponding {@link OperationHandler}s are
 * registered in a map and used to handle
 * provided operations - allowing different store implementations to handle the
 * same operations in their own store specific way.
 * Optional functionality can be added to store implementations defined by the
 * {@link uk.gov.gchq.gaffer.store.StoreTrait}s.
 */
public abstract class AbstractStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStore.class);
    private final Class<? extends Serialiser> requiredParentSerialiserClass;
    private final Map<Class<? extends Operation>, OperationHandler> operationHandlers = new LinkedHashMap<>();
    protected final List<OperationChainOptimiser> opChainOptimisers = new ArrayList<>();
    protected final OperationChainValidator opChainValidator;
    private final SchemaOptimiser schemaOptimiser;

    /**
     * The schema - contains the type of {@link uk.gov.gchq.gaffer.data.element.Element}s
     * to be stored and how to aggregate the elements.
     */
    private Schema schema;

    /**
     * The original schema containing all of the original descriptions and parent groups.
     */
    private Schema originalSchema;

    /**
     * The store properties - contains specific configuration information for
     * the store - such as database connection strings.
     */
    private StoreProperties properties;

    private GraphLibrary library;

    private JobTracker jobTracker;
    private ExecutorService executorService;
    private String graphId;

    public AbstractStore() {
        this.requiredParentSerialiserClass = getRequiredParentSerialiserClass();
        this.opChainValidator = createOperationChainValidator();
        this.schemaOptimiser = createSchemaOptimiser();
    }

    public static AbstractStore createStore(final String graphId, final byte[] schema, final Properties storeProperties) {
        return createStore(graphId, Schema.fromJson(schema), StoreProperties.loadStoreProperties(storeProperties));
    }

    public static AbstractStore createStore(final String graphId, final Schema schema, final StoreProperties storeProperties) {
        if (null == storeProperties) {
            throw new IllegalArgumentException("Store properties are required to create a store. graphId: " + graphId);
        }

        final String storeClass = storeProperties.getStoreClass();
        if (null == storeClass) {
            throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS + ", GraphId: " + graphId);
        }

        final AbstractStore newStore;
        try {
            newStore = Class.forName(storeClass)
                    .asSubclass(AbstractStore.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create store of type: " + storeClass, e);
        }

        try {
            newStore.initialise(graphId, schema, storeProperties);
        } catch (final StoreException e) {
            throw new IllegalArgumentException("Could not initialise the store with provided arguments.", e);
        }
        return newStore;
    }

    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        LOGGER.debug("Initialising {}", getClass().getSimpleName());
        if (null == graphId) {
            throw new IllegalArgumentException("graphId is required");
        }
        this.graphId = graphId;
        this.schema = schema;
        setProperties(properties);

        updateJsonSerialiser();

        startCacheServiceLoader(properties);
        this.jobTracker = createJobTracker();

        optimiseSchema();
        validateSchemas();
        addOpHandlers();
        addExecutorService();
    }

    public static void updateJsonSerialiser(final StoreProperties storeProperties) {
        if (null != storeProperties) {
            JSONSerialiser.update(
                    storeProperties.getJsonSerialiserClass(),
                    storeProperties.getJsonSerialiserModules(),
                    storeProperties.getStrictJson()
            );
        } else {
            JSONSerialiser.update();
        }
    }

    public void updateJsonSerialiser() {
        updateJsonSerialiser(getProperties());
    }

    /**
     * Returns true if the Store can handle the provided trait and false if it
     * cannot.
     *
     * @param storeTrait the Class of the Processor to be checked.
     * @return true if the Processor can be handled and false if it cannot.
     */
    public boolean hasTrait(final StoreTrait storeTrait) {
        final Set<StoreTrait> traits = getTraits();
        return null != traits && traits.contains(storeTrait);
    }

    /**
     * Returns the {@link uk.gov.gchq.gaffer.store.StoreTrait}s for this store.
     * Most stores should support FILTERING.
     * <p>
     * If you use Operation.validateFilter(Element) in you handlers, it will
     * deal with the filtering for you.
     * </p>
     *
     * @return the {@link uk.gov.gchq.gaffer.store.StoreTrait}s for this store.
     */
    public abstract Set<StoreTrait> getTraits();

    /**
     * Executes a given operation and returns the result.
     *
     * @param operation the operation to execute.
     * @param context   the context executing the operation
     * @throws OperationException thrown by the operation handler if the
     *                            operation fails.
     */
    public void execute(final Operation operation, final Context context) throws OperationException {
        execute(OperationChain.wrap(operation), context);
    }

    /**
     * Executes a given operation and returns the result.
     *
     * @param operation the operation to execute.
     * @param context   the context executing the operation
     * @param <O>       the output type of the operation
     * @return the result of executing the operation
     * @throws OperationException thrown by the operation handler if the
     *                            operation fails.
     */
    public <O> O execute(final Output<O> operation, final Context context) throws OperationException {
        return execute(OperationChain.wrap(operation), context);
    }

    protected <O> O execute(final OperationChain<O> operation, final Context context) throws OperationException {
        addOrUpdateJobDetail(operation, context, null, JobStatus.RUNNING);
        try {
            final O result = (O) handleOperation(operation, context);
            addOrUpdateJobDetail(operation, context, null, JobStatus.FINISHED);
            return result;
        } catch (final Throwable t) {
            addOrUpdateJobDetail(operation, context, t.getMessage(), JobStatus.FAILED);
            throw t;
        }
    }

    /**
     * Executes a given operation job and returns the job detail.
     *
     * @param operation the operation to execute.
     * @param context   the context executing the job
     * @return the job detail
     * @throws OperationException thrown if jobs are not configured.
     */
    public JobDetail executeJob(final Operation operation, final Context context) throws OperationException {
        return executeJob(OperationChain.wrap(operation), context);
    }

    protected JobDetail executeJob(final OperationChain<?> operationChain, final Context context) throws OperationException {
        if (null == jobTracker) {
            throw new OperationException("Running jobs has not configured.");
        }

        if (isSupported(ExportToGafferResultCache.class)) {
            boolean hasExport = false;
            for (final Operation operation : operationChain.getOperations()) {
                if (operation instanceof ExportToGafferResultCache) {
                    hasExport = true;
                    break;
                }
            }
            if (!hasExport) {
                operationChain.getOperations()
                        .add(new ExportToGafferResultCache());
            }
        }

        final JobDetail initialJobDetail = addOrUpdateJobDetail(operationChain, context, null, JobStatus.RUNNING);

        final Runnable runnable = () -> {
            try {
                handleOperation(operationChain, context);
                addOrUpdateJobDetail(operationChain, context, null, JobStatus.FINISHED);
            } catch (final Error e) {
                addOrUpdateJobDetail(operationChain, context, e.getMessage(), JobStatus.FAILED);
                throw e;
            } catch (final Exception e) {
                LOGGER.warn("Operation chain job failed to execute", e);
                addOrUpdateJobDetail(operationChain, context, e.getMessage(), JobStatus.FAILED);
            }
        };

        executorService.execute(runnable);

        return initialJobDetail;
    }

    public void runAsync(final Runnable runnable) {
        executorService.execute(runnable);
    }

    public JobTracker getJobTracker() {
        return jobTracker;
    }

    /**
     * @param operationClass the operation class to check
     * @return true if the provided operation is supported.
     */
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        final OperationHandler operationHandler = operationHandlers.get(operationClass);
        return null != operationHandler;
    }

    /**
     * @return a collection of all the supported {@link Operation}s.
     */
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return operationHandlers.keySet();
    }

    public Set<Class<? extends Operation>> getNextOperations(final Class<? extends Operation> operation) {
        if (null == operation || !Output.class.isAssignableFrom(operation)) {
            return getSupportedOperations();
        }

        final Set<Class<? extends Operation>> ops = new LinkedHashSet<>();
        if (Output.class.isAssignableFrom(operation)) {
            final Class<?> outputType = OperationUtil.getOutputType((Class) operation);
            for (final Class<? extends Operation> nextOp : getSupportedOperations()) {
                if (Input.class.isAssignableFrom(nextOp)) {
                    final Class<?> inputType = OperationUtil.getInputType((Class) nextOp);
                    if (OperationUtil.isValid(outputType, inputType)
                            .isValid()) {
                        ops.add(nextOp);
                    }
                }
            }
        }

        return ops;
    }

    /**
     * Ensures all identifier and property values are populated on an element by
     * triggering getters on the element for
     * all identifier and properties in the {@link Schema} forcing a lazy
     * element to load all of its values.
     *
     * @param lazyElement the lazy element
     * @return the fully populated unwrapped element
     */
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "Getters are called to trigger the loading data")
    public Element populateElement(final Element lazyElement) {
        final SchemaElementDefinition elementDefinition = getSchema().getElement(
                lazyElement.getGroup());
        if (null != elementDefinition) {
            for (final IdentifierType identifierType : elementDefinition.getIdentifiers()) {
                lazyElement.getIdentifier(identifierType);
            }

            for (final String propertyName : elementDefinition.getProperties()) {
                lazyElement.getProperty(propertyName);
            }
        }

        return lazyElement.getElement();
    }

    public String getGraphId() {
        return graphId;
    }

    /**
     * Get this Store's {@link Schema}.
     *
     * @return the instance of {@link Schema} used for describing the type of
     * {@link uk.gov.gchq.gaffer.data.element.Element}s to be stored and how to
     * aggregate the elements.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Get this Store's {@link uk.gov.gchq.gaffer.store.StoreProperties}.
     *
     * @return the instance of {@link uk.gov.gchq.gaffer.store.StoreProperties},
     * this may contain details such as database connection details.
     */
    public StoreProperties getProperties() {
        return properties;
    }

    protected void setProperties(final StoreProperties properties) {
        final Class<? extends StoreProperties> requiredPropsClass = getPropertiesClass();
        properties.updateStorePropertiesClass(requiredPropsClass);

        // If the properties instance is not already an instance of the required class then reload the properties
        if (requiredPropsClass.isAssignableFrom(properties.getClass())) {
            this.properties = properties;
        } else {
            this.properties = StoreProperties.loadStoreProperties(properties.getProperties());
        }

        ReflectionUtil.addReflectionPackages(properties.getReflectionPackages());
        updateJsonSerialiser();
    }

    public GraphLibrary getGraphLibrary() {
        return library;
    }

    public void setGraphLibrary(final GraphLibrary library) {
        this.library = library;
    }

    public void optimiseSchema() {
        schema = schemaOptimiser.optimise(schema, hasTrait(StoreTrait.ORDERED));
    }

    public void validateSchemas() {
        final ValidationResult validationResult = new ValidationResult();
        if (null == schema) {
            validationResult.addError("Schema is missing");
        } else {
            validationResult.add(schema.validate());

            getSchemaElements().forEach((key, value) -> value
                    .getProperties()
                    .forEach(propertyName -> {
                        final Class propertyClass = value
                                .getPropertyClass(propertyName);
                        final Serialiser serialisation = value
                                .getPropertyTypeDef(propertyName)
                                .getSerialiser();

                        if (null == serialisation) {
                            validationResult.addError(
                                    String.format("Could not find a serialiser for property '%s' in the group '%s'.", propertyName, key));
                        } else if (!serialisation.canHandle(propertyClass)) {
                            validationResult.addError(String.format("Schema serialiser (%s) for property '%s' in the group '%s' cannot handle property found in the schema", serialisation
                                    .getClass()
                                    .getName(), propertyName, key));
                        }
                    }));

            validateSchema(validationResult, getSchema().getVertexSerialiser());

            getSchema().getTypes()
                    .forEach((k, v) -> validateSchema(validationResult, v.getSerialiser()));
        }

        if (!validationResult.isValid()) {
            throw new SchemaException("Schema is not valid. "
                    + validationResult.getErrorString());
        }
    }

    public Context createContext(final User user) {
        return new Context(user);
    }

    protected Class<? extends StoreProperties> getPropertiesClass() {
        return StoreProperties.class;
    }

    /**
     * Throws a {@link SchemaException} if the Vertex Serialiser is
     * inconsistent.
     */
    protected void validateConsistentVertex() {
        if (null != getSchema().getVertexSerialiser() && !getSchema().getVertexSerialiser()
                .isConsistent()) {
            throw new SchemaException("Vertex serialiser is inconsistent. This store requires vertices to be serialised in a consistent way.");
        }
    }

    /**
     * Ensures that each of the GroupBy properties in the {@link
     * SchemaElementDefinition} is consistent,
     * otherwise an error is added to the {@link ValidationResult}.
     *
     * @param schemaElementDefinitionEntry A map of SchemaElementDefinitions
     * @param validationResult             The validation result
     */
    protected void validateConsistentGroupByProperties(final Map.Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry, final ValidationResult validationResult) {
        for (final String property : schemaElementDefinitionEntry.getValue()
                .getGroupBy()) {
            final TypeDefinition propertyTypeDef = schemaElementDefinitionEntry.getValue()
                    .getPropertyTypeDef(property);
            if (null != propertyTypeDef) {
                final Serialiser serialiser = propertyTypeDef.getSerialiser();
                if (null != serialiser && !serialiser.isConsistent()) {
                    validationResult.addError("Serialiser for groupBy property: " + property
                            + " is inconsistent. This store requires all groupBy property serialisers to be consistent. Serialiser "
                            + serialiser.getClass().getName() + " is not consistent.");
                }
            }
        }
    }

    protected void validateSchemaElementDefinition(final Map.Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry, final ValidationResult validationResult) {
        schemaElementDefinitionEntry.getValue()
                .getProperties()
                .forEach(propertyName -> {
                    final Class propertyClass = schemaElementDefinitionEntry.getValue().getPropertyClass(propertyName);
                    final Serialiser serialisation = schemaElementDefinitionEntry.getValue().getPropertyTypeDef(propertyName).getSerialiser();

                    if (null == serialisation) {
                        validationResult.addError(
                                String.format("Could not find a serialiser for property '%s' in the group '%s'.", propertyName, schemaElementDefinitionEntry.getKey()));
                    } else if (!serialisation.canHandle(propertyClass)) {
                        validationResult.addError(String.format("Schema serialiser (%s) for property '%s' in the group '%s' cannot handle property found in the schema",
                                serialisation.getClass().getName(), propertyName, schemaElementDefinitionEntry.getKey()));
                    }
                });
    }

    protected void validateSchema(final ValidationResult validationResult, final Serialiser serialiser) {
        if ((null != serialiser) && !requiredParentSerialiserClass.isInstance(serialiser)) {
            validationResult.addError(
                    String.format("Schema serialiser (%s) is not instance of %s",
                            serialiser.getClass().getSimpleName(),
                            requiredParentSerialiserClass.getSimpleName()));
        }
    }

    protected JobTracker createJobTracker() {
        if (properties.getJobTrackerEnabled()) {
            return new JobTracker();
        }
        return null;
    }

    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser();
    }

    protected OperationChainValidator createOperationChainValidator() {
        return new OperationChainValidator(new ViewValidator());
    }

    public OperationChainValidator getOperationChainValidator() {
        return opChainValidator;
    }

    public void addOperationChainOptimisers(final List<OperationChainOptimiser> newOpChainOptimisers) {
        opChainOptimisers.addAll(newOpChainOptimisers);
    }

    /**
     * Any additional operations that a store can handle should be registered in
     * this method by calling addOperationHandler(...)
     */
    protected abstract void addAdditionalOperationHandlers();

    /**
     * Get this Stores implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.impl.get.GetElements}. All Stores must
     * implement this.
     *
     * @return the implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.impl.get.GetElements}
     */
    protected abstract OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.impl.get.GetAllElements}. All Stores must
     * implement this.
     *
     * @return the implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.impl.get.GetAllElements}
     */
    protected abstract OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link
     * GetAdjacentIds}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link GetAdjacentIds}
     */
    protected abstract OutputOperationHandler<? extends GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler();

    /**
     * Get this Stores implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.impl.add.AddElements}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.impl.add.AddElements}
     */
    protected abstract OperationHandler<? extends AddElements> getAddElementsHandler();

    /**
     * Get this Store's implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.OperationChain}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link
     * uk.gov.gchq.gaffer.operation.OperationChain}
     */
    protected OperationHandler<? extends OperationChain<?>> getOperationChainHandler() {
        return new OperationChainHandler<>(opChainValidator, opChainOptimisers);
    }

    protected HashMap<String, SchemaElementDefinition> getSchemaElements() {
        final HashMap<String, SchemaElementDefinition> schemaElements = new HashMap<>();
        schemaElements.putAll(getSchema().getEdges());
        schemaElements.putAll(getSchema().getEntities());
        return schemaElements;
    }

    protected abstract Class<? extends Serialiser> getRequiredParentSerialiserClass();

    /**
     * Should deal with any unhandled operations, simply throws an {@link
     * UnsupportedOperationException}.
     *
     * @param operation the operation that does not have a registered handler.
     * @param context   operation execution context
     * @return the result of the operation.
     */
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        throw new UnsupportedOperationException("Operation " + operation.getClass() + " is not supported by the " + getClass()
                .getSimpleName() + '.');
    }

    public void addOperationHandler(final Class<? extends Operation> opClass, final OperationHandler handler) {
        if (null == handler) {
            operationHandlers.remove(opClass);
        } else {
            operationHandlers.put(opClass, handler);
        }
    }

    public <OP extends Output<O>, O> void addOperationHandler(final Class<? extends Output<O>> opClass, final OutputOperationHandler<OP, O> handler) {
        if (null == handler) {
            operationHandlers.remove(opClass);
        } else {
            operationHandlers.put(opClass, handler);
        }
    }

    public OperationHandler<Operation> getOperationHandler(final Class<? extends Operation> opClass) {
        return operationHandlers.get(opClass);
    }

    private JobDetail addOrUpdateJobDetail(final OperationChain<?> operationChain, final Context context, final String msg, final JobStatus jobStatus) {
        final JobDetail newJobDetail = new JobDetail(context.getJobId(), context
                .getUser()
                .getUserId(), operationChain, jobStatus, msg);
        if (null != jobTracker) {
            final JobDetail oldJobDetail = jobTracker.getJob(newJobDetail.getJobId(), context
                    .getUser());
            if (null == oldJobDetail) {
                jobTracker.addOrUpdateJob(newJobDetail, context.getUser());
            } else {
                jobTracker.addOrUpdateJob(new JobDetail(oldJobDetail, newJobDetail), context
                        .getUser());
            }
        }
        return newJobDetail;
    }

    public Object handleOperation(final Operation operation, final Context context) throws
            OperationException {
        final OperationHandler<Operation> handler = getOperationHandler(
                operation.getClass());
        Object result;
        try {
            if (null != handler) {
                result = handler.doOperation(operation, context, this);
            } else {
                result = doUnhandledOperation(operation, context);
            }
        } catch (final Exception e) {
            CloseableUtil.close(operation);
            throw e;
        }

        if (null == result) {
            CloseableUtil.close(operation);
        }

        return result;
    }

    private void addExecutorService() {
        final Integer jobExecutorThreadCount = getProperties().getJobExecutorThreadCount();
        LOGGER.debug("Initialising ExecutorService with " + jobExecutorThreadCount + " threads");
        this.executorService = Executors.newFixedThreadPool(jobExecutorThreadCount, runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        });
    }

    protected void addOpHandlers() {
        addAdditionalOperationHandlers();
        addConfiguredOperationHandlers();
    }


    private void addConfiguredOperationHandlers() {
        final OperationDeclarations declarations = getProperties().getOperationDeclarations();
        if (null != declarations) {
            for (final OperationDeclaration definition : declarations.getOperations()) {
                addOperationHandler(definition.getOperation(), definition.getHandler());
            }
        }
    }

    protected void startCacheServiceLoader(final StoreProperties properties) {
        CacheServiceLoader.initialise(properties.getProperties());
    }

    public void setOriginalSchema(final Schema originalSchema) {
        this.originalSchema = originalSchema;
    }

    public Schema getOriginalSchema() {
        return originalSchema;
    }

}
