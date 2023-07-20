/*
 * Copyright 2016-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.ExecutorService;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
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
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.impl.GetVariable;
import uk.gov.gchq.gaffer.operation.impl.GetVariables;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Limit;
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
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.HasTrait;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.OperationUtil;
import uk.gov.gchq.gaffer.store.operation.add.AddSchemaToLibrary;
import uk.gov.gchq.gaffer.store.operation.add.AddStorePropertiesToLibrary;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.AddSchemaToLibraryHandler;
import uk.gov.gchq.gaffer.store.operation.handler.AddStorePropertiesToLibraryHandler;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.CountHandler;
import uk.gov.gchq.gaffer.store.operation.handler.DiscardOutputHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ForEachHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetSchemaHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetVariableHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetVariablesHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetWalksHandler;
import uk.gov.gchq.gaffer.store.operation.handler.HasTraitHandler;
import uk.gov.gchq.gaffer.store.operation.handler.IfHandler;
import uk.gov.gchq.gaffer.store.operation.handler.LimitHandler;
import uk.gov.gchq.gaffer.store.operation.handler.MapHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ReduceHandler;
import uk.gov.gchq.gaffer.store.operation.handler.SetVariableHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ValidateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ValidateOperationChainHandler;
import uk.gov.gchq.gaffer.store.operation.handler.WhileHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MaxHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MinHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.SortHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.GetExportsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.ExportToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.GetSetExportHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.AggregateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.FilterHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.TransformHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.CancelScheduledJobHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetAllJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobResultsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.join.JoinHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedOperationsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedViewsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.NamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToArrayHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToCsvHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToEntitySeedsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToListHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToMapHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToSingletonListHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToStreamHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToVerticesHandler;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.StreamSupport;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

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
public abstract class Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(Store.class);
    private final Class<? extends Serialiser> requiredParentSerialiserClass;
    private final Map<Class<? extends Operation>, OperationHandler> operationHandlers = new LinkedHashMap<>();
    protected final List<OperationChainOptimiser> opChainOptimisers = new ArrayList<>();
    protected final OperationChainValidator opChainValidator;
    private final SchemaOptimiser schemaOptimiser;
    private final Boolean addCoreOpHandlers;

    /**
     * The schema - contains the type of {@link uk.gov.gchq.gaffer.data.element.Element}s
     * to be stored and how to aggregate the elements.
     */
    private Schema schema;

    /**
     * The original user supplied schema containing all the original descriptions and parent
     * groups. In most cases this will differ from the schema used internally which is optimised.
     */
    private Schema originalSchema;

    /**
     * The store properties - contains specific configuration information for
     * the store - such as database connection strings.
     */
    private StoreProperties properties;

    private GraphLibrary library;

    private JobTracker jobTracker;
    private String graphId;

    private boolean jobsRescheduled;

    public Store() {
        this(true);
    }

    public Store(final Boolean addCoreOpHandlers) {
        this.addCoreOpHandlers = addCoreOpHandlers;
        this.requiredParentSerialiserClass = getRequiredParentSerialiserClass();
        this.opChainValidator = createOperationChainValidator();
        this.schemaOptimiser = createSchemaOptimiser();
    }

    public static Store createStore(final String graphId, final byte[] schema, final Properties storeProperties) {
        return createStore(graphId, Schema.fromJson(schema), StoreProperties.loadStoreProperties(storeProperties));
    }

    public static Store createStore(final String graphId, final Schema schema, final StoreProperties storeProperties) {
        if (isNull(storeProperties)) {
            throw new IllegalArgumentException(String.format("Store properties are required to create a store. graphId: %s", graphId));
        }

        final String storeClass = storeProperties.getStoreClass();
        if (isNull(storeClass)) {
            throw new IllegalArgumentException(String.format("The Store class name was not found in the store properties for key: %s, GraphId: %s", StoreProperties.STORE_CLASS, graphId));
        }

        final Store newStore;
        try {
            newStore = Class.forName(storeClass)
                    .asSubclass(Store.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format("Could not create store of type: %s", storeClass), e);
        }

        try {
            newStore.initialise(graphId, schema, storeProperties);
        } catch (final StoreException e) {
            throw new IllegalArgumentException("Could not initialise the store with provided arguments.", e);
        }
        return newStore;
    }

    public void initialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        LOGGER.debug("Initialising {}", getClass().getSimpleName());
        if (isNull(graphId)) {
            throw new IllegalArgumentException("graphId is required");
        }
        this.graphId = graphId;
        this.schema = schema;
        setProperties(properties);

        updateJsonSerialiser();

        startCacheServiceLoader(properties);
        this.jobTracker = createJobTracker();

        addOpHandlers();
        optimiseSchema();
        validateSchemas();
        addExecutorService(properties);

        if (properties.getJobTrackerEnabled() && !jobsRescheduled) {
            Iterable<JobDetail> scheduledJobs = null;
            try {
                scheduledJobs = this.jobTracker.getAllScheduledJobs();
                if (scheduledJobs != null) {
                    StreamSupport.stream(scheduledJobs.spliterator(), false)
                            .peek(jd -> LOGGER.debug("Rescheduling job: {}", jd))
                            .forEach(this::rescheduleJob);
                }
            } finally {
                CloseableUtil.close(scheduledJobs);
            }

            jobsRescheduled = true;
        }
    }

    private void rescheduleJob(final JobDetail jobDetail) {

        try {

            final OperationChain<?> operationChain = JSONSerialiser.deserialise(jobDetail.getSerialisedOperationChain(),
                    OperationChain.class);
            final Context context = new Context(jobDetail.getUser());
            context.setOriginalOpChain(operationChain);

            getExecutorService().scheduleAtFixedRate(new ScheduledJobRunnable(operationChain, jobDetail, context),
                    jobDetail.getRepeat().getInitialDelay(),
                    jobDetail.getRepeat().getRepeatPeriod(),
                    jobDetail.getRepeat().getTimeUnit());

        } catch (final SerialisationException exception) {
            throw new RuntimeException(exception);
        }
    }

    public static void updateJsonSerialiser(final StoreProperties storeProperties) {
        if (nonNull(storeProperties)) {
            JSONSerialiser.update(storeProperties.getJsonSerialiserClass(),
                    storeProperties.getJsonSerialiserModules(),
                    storeProperties.getStrictJson());
        } else {
            JSONSerialiser.update();
        }
    }

    public void updateJsonSerialiser() {
        updateJsonSerialiser(getProperties());
    }

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
     * @param context   the context executing the job.
     * @return the job detail.
     * @throws OperationException thrown if jobs are not configured.
     */
    public JobDetail executeJob(final Operation operation, final Context context) throws OperationException {
        return executeJob(OperationChain.wrap(operation), context);
    }

    /**
     * Executes a given {@link Job} containing an Operation and/or
     * {@link uk.gov.gchq.gaffer.jobtracker.Repeat} and returns the job detail.
     *
     * @param job     the job to execute.
     * @param context the context executing the job.
     * @return the job detail.
     * @throws OperationException thrown if there is an error running the job.
     */
    public JobDetail executeJob(final Job job, final Context context) throws OperationException {
        final OperationChain opChain = OperationChain.wrap(job.getOperation());
        if (opChain.getOperations().isEmpty()) {
            throw new IllegalArgumentException("An operation is required");
        }
        final JobDetail jobDetail = addOrUpdateJobDetail(opChain, context, null, JobStatus.RUNNING);
        jobDetail.setRepeat(job.getRepeat());
        return executeJob(opChain, jobDetail, context);
    }

    protected JobDetail executeJob(final OperationChain<?> operationChain, final Context context)
            throws OperationException {
        final JobDetail jobDetail = addOrUpdateJobDetail(operationChain, context, null, JobStatus.RUNNING);
        return executeJob(operationChain, jobDetail, context);
    }

    protected JobDetail executeJob(final OperationChain<?> operationChain, final Context context, final String parentJobId)
            throws OperationException {
        final JobDetail childJobDetail = addOrUpdateJobDetail(operationChain, context, null, JobStatus.RUNNING);
        childJobDetail.setParentJobId(parentJobId);
        return executeJob(operationChain, childJobDetail, context);
    }

    private JobDetail executeJob(final Operation operation,
                                 final JobDetail jobDetail,
                                 final Context context)
            throws OperationException {
        if (isNull(jobTracker)) {
            throw new OperationException("JobTracker has not been configured.");
        }

        if (isNull(ExecutorService.getService()) || !ExecutorService.isEnabled()) {
            throw new OperationException(("Executor Service is not enabled."));
        }

        if (nonNull(jobDetail.getRepeat())) {
            return scheduleJob(operation, jobDetail, context);
        } else {
            return runJob(operation, jobDetail, context);
        }

    }

    private JobDetail scheduleJob(final Operation operation,
                                  final JobDetail parentJobDetail,
                                  final Context context) {
        final OperationChain<?> clonedOp = (operation instanceof Operations)
                ? (OperationChain) operation.shallowClone()
                : OperationChain.wrap(operation).shallowClone();

        getExecutorService().scheduleAtFixedRate(new ScheduledJobRunnable(clonedOp, parentJobDetail, context),
                parentJobDetail.getRepeat().getInitialDelay(),
                parentJobDetail.getRepeat().getRepeatPeriod(),
                parentJobDetail.getRepeat().getTimeUnit());

        return addOrUpdateJobDetail(clonedOp, context, null, JobStatus.SCHEDULED_PARENT);
    }

    class ScheduledJobRunnable implements Runnable {

        private final OperationChain<?> operationChain;
        private final JobDetail jobDetail;
        private final Context context;

        ScheduledJobRunnable(final OperationChain<?> operationChain, final JobDetail jobDetail, final Context context) {

            this.operationChain = operationChain;
            this.jobDetail = jobDetail;
            this.context = context;
        }

        OperationChain<?> getOperationChain() {
            return operationChain;
        }

        JobDetail getJobDetail() {
            return jobDetail;
        }

        Context getContext() {
            return context;
        }

        @Override
        public void run() {
            if ((jobTracker.getJob(jobDetail.getJobId(), context.getUser()).getStatus().equals(JobStatus.CANCELLED))) {
                Thread.currentThread().interrupt();
                return;
            }
            final Context newContext = context.shallowClone();
            try {
                executeJob(operationChain, newContext, jobDetail.getJobId());
            } catch (final OperationException e) {
                throw new RuntimeException("Exception within scheduled job", e);
            }
        }
    }

    private JobDetail runJob(final Operation operation,
                             final JobDetail jobDetail,
                             final Context context) {
        final OperationChain<?> clonedOp = (operation instanceof Operations)
                ? (OperationChain) operation.shallowClone()
                : OperationChain.wrap(operation).shallowClone();

        if (isSupported(ExportToGafferResultCache.class)) {
            boolean hasExport = false;
            for (final Operation op : clonedOp.getOperations()) {
                if (op instanceof ExportToGafferResultCache) {
                    hasExport = true;
                    break;
                }
            }
            if (!hasExport) {
                clonedOp.getOperations()
                        .add(new ExportToGafferResultCache());
            }
        }

        runAsync(() -> {
            try {
                handleOperation(clonedOp, context);
                addOrUpdateJobDetail(clonedOp, context, null, JobStatus.FINISHED);
            } catch (final Error e) {
                addOrUpdateJobDetail(clonedOp, context, e.getMessage(),
                        JobStatus.FAILED);
                throw e;
            } catch (final Exception e) {
                LOGGER.warn("Operation chain job failed to execute", e);
                addOrUpdateJobDetail(clonedOp, context, e.getMessage(),
                        JobStatus.FAILED);
            }
        });
        return jobDetail;
    }

    public void runAsync(final Runnable runnable) {
        getExecutorService().execute(runnable);
    }

    protected ScheduledExecutorService getExecutorService() {
        return (nonNull(ExecutorService.getService()) && ExecutorService.isEnabled())
                ? ExecutorService.getService()
                : null;
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
        return nonNull(operationHandler);
    }

    /**
     * @return a collection of all the supported {@link Operation}s.
     */
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return new HashSet<>(operationHandlers.keySet());
    }

    public Set<Class<? extends Operation>> getNextOperations(final Class<? extends Operation> operation) {
        if (isNull(operation) || !Output.class.isAssignableFrom(operation)) {
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
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification = "Getters are called to trigger the loading data")
    public Element populateElement(final Element lazyElement) {
        final SchemaElementDefinition elementDefinition = getSchema().getElement(
                lazyElement.getGroup());
        if (nonNull(elementDefinition)) {
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
     * Get this Store's internal {@link Schema}.
     * <p>
     * This is optimised and will usually differ from the original schema used to
     * create the store initially.
     *
     * @return the instance of {@link Schema} used for describing the type of
     * {@link uk.gov.gchq.gaffer.data.element.Element}s to be stored and how to
     * aggregate the elements.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Set the Store's original {@link Schema}.
     * <p>
     * This is used to keep an (unoptimised, unchanged) copy of the original
     * schema used to create the store initially.
     *
     * @param originalSchema the {@link Schema} to be set as the original
     *                       schema used to create this store
     */
    public void setOriginalSchema(final Schema originalSchema) {
        this.originalSchema = originalSchema;
    }

    /**
     * Get the Store's original {@link Schema}.
     *
     * @return the {@link Schema} used to create this store, null if not set
     */
    public Schema getOriginalSchema() {
        return originalSchema;
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
        Boolean isOrdered;
        try {
            isOrdered = execute(new HasTrait.Builder()
                    .trait(StoreTrait.ORDERED)
                    .currentTraits(false)
                    .build(), new Context());
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Error performing HasTrait Operation while optimising schema.", e);
        }
        schema = schemaOptimiser.optimise(schema, isOrdered != null && isOrdered);
    }

    public void validateSchemas() {
        final ValidationResult validationResult = new ValidationResult();
        if (isNull(schema)) {
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

                        if (isNull(serialisation)) {
                            validationResult.addError(String.format("Could not find a serialiser for property '%s' in the group '%s'.",
                                    propertyName, key));
                        } else if (!serialisation.canHandle(propertyClass)) {
                            validationResult.addError(String
                                    .format("Schema serialiser (%s) for property '%s' in the group '%s' cannot handle property found in the schema",
                                            serialisation.getClass().getName(),
                                            propertyName, key));
                        }
                    }));

            validateSchema(validationResult, getSchema().getVertexSerialiser());

            getSchema().getTypes().forEach((k, v) -> validateSchema(validationResult, v.getSerialiser()));
        }

        if (!validationResult.isValid()) {
            throw new SchemaException(String.format("Schema is not valid. %s", validationResult.getErrorString()));
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
        if (nonNull(getSchema().getVertexSerialiser())
                && !getSchema().getVertexSerialiser().isConsistent()) {
            throw new SchemaException("Vertex serialiser is inconsistent. This store requires vertices to be serialised in a consistent way.");
        }
    }

    /**
     * Ensures that each of the GroupBy properties in the {@link SchemaElementDefinition} is consistent,
     * otherwise an error is added to the {@link ValidationResult}.
     *
     * @param schemaElementDefinitionEntry A map of SchemaElementDefinitions
     * @param validationResult             The validation result
     */
    protected void validateConsistentGroupByProperties(final Map.Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry,
                                                       final ValidationResult validationResult) {
        for (final String property : schemaElementDefinitionEntry.getValue().getGroupBy()) {
            final TypeDefinition propertyTypeDef = schemaElementDefinitionEntry.getValue().getPropertyTypeDef(property);
            if (nonNull(propertyTypeDef)) {
                final Serialiser serialiser = propertyTypeDef.getSerialiser();
                if (nonNull(serialiser) && !serialiser.isConsistent()) {
                    validationResult.addError("Serialiser for groupBy property: " + property
                            + " is inconsistent. This store requires all groupBy property serialisers to be consistent. Serialiser "
                            + serialiser.getClass().getName() + " is not consistent.");
                }
            }
        }
    }

    protected void validateSchemaElementDefinition(final Map.Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry,
                                                   final ValidationResult validationResult) {
        schemaElementDefinitionEntry.getValue()
                .getProperties()
                .forEach(propertyName -> {
                    final Class propertyClass = schemaElementDefinitionEntry.getValue().getPropertyClass(propertyName);
                    final Serialiser serialisation = schemaElementDefinitionEntry.getValue()
                            .getPropertyTypeDef(propertyName).getSerialiser();

                    if (isNull(serialisation)) {
                        validationResult.addError(String.format("Could not find a serialiser for property '%s' in the group '%s'.",
                                propertyName, schemaElementDefinitionEntry.getKey()));
                    } else if (!serialisation.canHandle(propertyClass)) {
                        validationResult.addError(String
                                .format("Schema serialiser (%s) for property '%s' in the group '%s' cannot handle property found in the schema",
                                        serialisation.getClass().getName(), propertyName,
                                        schemaElementDefinitionEntry.getKey()));
                    }
                });
    }

    protected void validateSchema(final ValidationResult validationResult, final Serialiser serialiser) {
        if ((nonNull(serialiser)) && !requiredParentSerialiserClass.isInstance(serialiser)) {
            validationResult.addError(String.format("Schema serialiser (%s) is not instance of %s",
                    serialiser.getClass().getSimpleName(),
                    requiredParentSerialiserClass.getSimpleName()));
        }
    }

    protected JobTracker createJobTracker() {
        if (properties.getJobTrackerEnabled()) {
            return new JobTracker(getProperties().getCacheServiceNameSuffix());
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

    public List<OperationChainOptimiser> getOperationChainOptimisers() {
        return unmodifiableList(opChainOptimisers);
    }

    /**
     * Any additional operations that a store can handle should be registered in
     * this method by calling addOperationHandler(...)
     */
    protected abstract void addAdditionalOperationHandlers();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements}. All Stores must
     * implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements}
     */
    protected abstract OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetAllElements}. All Stores must
     * implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetAllElements}
     */
    protected abstract OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link GetAdjacentIds}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link GetAdjacentIds}
     */
    protected abstract OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.add.AddElements}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.add.AddElements}
     */
    protected abstract OperationHandler<? extends AddElements> getAddElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.store.operation.GetTraits}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.store.operation.GetTraits}
     */
    protected abstract OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler();

    /**
     * Get this Store's implementation of the handler for {@link uk.gov.gchq.gaffer.operation.OperationChain}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.OperationChain}
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

    @SuppressWarnings({"rawtypes"})
    protected abstract Class<? extends Serialiser> getRequiredParentSerialiserClass();

    /**
     * Should deal with any unhandled operations, simply throws an {@link UnsupportedOperationException}.
     *
     * @param operation the operation that does not have a registered handler.
     * @param context   operation execution context
     * @return the result of the operation.
     */
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        throw new UnsupportedOperationException(String.format("Operation %s is not supported by the %s.", operation.getClass(),
                getClass().getSimpleName()));
    }

    public void addOperationHandler(final Class<? extends Operation> opClass, final OperationHandler handler) {
        if (isNull(handler)) {
            operationHandlers.remove(opClass);
        } else {
            operationHandlers.put(opClass, handler);
        }
    }

    public <OP extends Output<O>, O> void addOperationHandler(final Class<? extends Output<O>> opClass,
                                                              final OutputOperationHandler<OP, O> handler) {
        if (isNull(handler)) {
            operationHandlers.remove(opClass);
        } else {
            operationHandlers.put(opClass, handler);
        }
    }

    public OperationHandler<Operation> getOperationHandler(final Class<? extends Operation> opClass) {
        return operationHandlers.get(opClass);
    }

    private JobDetail addOrUpdateJobDetail(final OperationChain<?> operationChain, final Context context,
                                           final String msg, final JobStatus jobStatus) {
        final JobDetail newJobDetail = new JobDetail(context.getJobId(), context.getUser(), operationChain, jobStatus, msg);
        if (nonNull(jobTracker)) {
            final JobDetail oldJobDetail = jobTracker.getJob(newJobDetail.getJobId(), context.getUser());
            if (newJobDetail.getStatus().equals(JobStatus.SCHEDULED_PARENT)) {
                newJobDetail.setRepeat(null);
                newJobDetail.setSerialisedOperationChain(operationChain);
            }

            if (isNull(oldJobDetail)) {
                jobTracker.addOrUpdateJob(newJobDetail, context.getUser());
            } else {
                jobTracker.addOrUpdateJob(new JobDetail(oldJobDetail, newJobDetail), context
                        .getUser());
            }
        }
        return newJobDetail;
    }

    public Object handleOperation(final Operation operation, final Context context) throws OperationException {
        final OperationHandler<Operation> handler = getOperationHandler(operation.getClass());
        Object result;
        try {
            if (nonNull(handler)) {
                result = handler.doOperation(operation, context, this);
            } else {
                result = doUnhandledOperation(operation, context);
            }
        } catch (final Exception e) {
            CloseableUtil.close(operation);
            throw e;
        }

        if (isNull(result)) {
            CloseableUtil.close(operation);
        }

        return result;
    }

    private void addExecutorService(final StoreProperties properties) {
        ExecutorService.initialise(properties.getJobExecutorThreadCount());
    }

    private void addOpHandlers() {
        if (addCoreOpHandlers) {
            addCoreOpHandlers();
        }
        addAdditionalOperationHandlers();
        addConfiguredOperationHandlers();
    }

    private void addCoreOpHandlers() {
        // Add elements
        addOperationHandler(AddElements.class, getAddElementsHandler());

        // Get Elements
        addOperationHandler(GetElements.class, getGetElementsHandler());

        // Get Adjacent
        addOperationHandler(GetAdjacentIds.class, getAdjacentIdsHandler());

        // Get All Elements
        addOperationHandler(GetAllElements.class, getGetAllElementsHandler());

        // Export
        addOperationHandler(ExportToSet.class, new ExportToSetHandler());
        addOperationHandler(GetSetExport.class, new GetSetExportHandler());
        addOperationHandler(GetExports.class, new GetExportsHandler());

        // Jobs
        if (nonNull(getJobTracker())) {
            addOperationHandler(GetJobDetails.class, new GetJobDetailsHandler());
            addOperationHandler(GetAllJobDetails.class, new GetAllJobDetailsHandler());
            addOperationHandler(GetJobResults.class, new GetJobResultsHandler());
            addOperationHandler(CancelScheduledJob.class, new CancelScheduledJobHandler());
        }

        // Output
        addOperationHandler(ToArray.class, new ToArrayHandler<>());
        addOperationHandler(ToEntitySeeds.class, new ToEntitySeedsHandler());
        addOperationHandler(ToList.class, new ToListHandler<>());
        addOperationHandler(ToMap.class, new ToMapHandler());
        addOperationHandler(ToCsv.class, new ToCsvHandler());
        addOperationHandler(ToSet.class, new ToSetHandler<>());
        addOperationHandler(ToStream.class, new ToStreamHandler<>());
        addOperationHandler(ToVertices.class, new ToVerticesHandler());

        if (nonNull(CacheServiceLoader.getService())) {
            // Named operation
            addOperationHandler(NamedOperation.class, new NamedOperationHandler());
            addOperationHandler(AddNamedOperation.class, new AddNamedOperationHandler(properties.getCacheServiceNameSuffix(graphId)));
            addOperationHandler(GetAllNamedOperations.class, new GetAllNamedOperationsHandler(properties.getCacheServiceNameSuffix(graphId)));
            addOperationHandler(DeleteNamedOperation.class, new DeleteNamedOperationHandler(properties.getCacheServiceNameSuffix(graphId)));

            // Named view
            addOperationHandler(AddNamedView.class, new AddNamedViewHandler(properties.getCacheServiceNameSuffix(graphId)));
            addOperationHandler(GetAllNamedViews.class, new GetAllNamedViewsHandler(properties.getCacheServiceNameSuffix(graphId)));
            addOperationHandler(DeleteNamedView.class, new DeleteNamedViewHandler(properties.getCacheServiceNameSuffix(graphId)));
        }

        // ElementComparison
        addOperationHandler(Max.class, new MaxHandler());
        addOperationHandler(Min.class, new MinHandler());
        addOperationHandler(Sort.class, new SortHandler());

        // OperationChain
        addOperationHandler(OperationChain.class, getOperationChainHandler());
        addOperationHandler(OperationChainDAO.class, getOperationChainHandler());

        // OperationChain validation
        addOperationHandler(ValidateOperationChain.class, new ValidateOperationChainHandler());

        // Walk tracking
        addOperationHandler(GetWalks.class, new GetWalksHandler());

        // Other
        addOperationHandler(GenerateElements.class, new GenerateElementsHandler<>());
        addOperationHandler(GenerateObjects.class, new GenerateObjectsHandler<>());
        addOperationHandler(Validate.class, new ValidateHandler());
        addOperationHandler(Count.class, new CountHandler());
        addOperationHandler(CountGroups.class, new CountGroupsHandler());
        addOperationHandler(Limit.class, new LimitHandler());
        addOperationHandler(DiscardOutput.class, new DiscardOutputHandler());
        addOperationHandler(GetSchema.class, new GetSchemaHandler());
        addOperationHandler(uk.gov.gchq.gaffer.operation.impl.Map.class, new MapHandler());
        addOperationHandler(If.class, new IfHandler());
        addOperationHandler(While.class, new WhileHandler());
        addOperationHandler(ForEach.class, new ForEachHandler());
        addOperationHandler(ToSingletonList.class, new ToSingletonListHandler());
        addOperationHandler(Reduce.class, new ReduceHandler());
        addOperationHandler(Join.class, new JoinHandler());

        // Context variables
        addOperationHandler(SetVariable.class, new SetVariableHandler());
        addOperationHandler(GetVariable.class, new GetVariableHandler());
        addOperationHandler(GetVariables.class, new GetVariablesHandler());

        // Function
        addOperationHandler(Filter.class, new FilterHandler());
        addOperationHandler(Transform.class, new TransformHandler());
        addOperationHandler(Aggregate.class, new AggregateHandler());

        // GraphLibrary Adds
        if (nonNull(getGraphLibrary()) && !(getGraphLibrary() instanceof NoGraphLibrary)) {
            addOperationHandler(AddSchemaToLibrary.class, new AddSchemaToLibraryHandler());
            addOperationHandler(AddStorePropertiesToLibrary.class, new AddStorePropertiesToLibraryHandler());
        }

        // Traits
        addOperationHandler(HasTrait.class, new HasTraitHandler());
        addOperationHandler(GetTraits.class, getGetTraitsHandler());

    }

    private void addConfiguredOperationHandlers() {
        final OperationDeclarations declarations = getProperties().getOperationDeclarations();
        if (nonNull(declarations)) {
            for (final OperationDeclaration definition : declarations.getOperations()) {
                addOperationHandler(definition.getOperation(), definition.getHandler());
            }
        }
    }

    protected void startCacheServiceLoader(final StoreProperties properties) {
        CacheServiceLoader.initialise(properties.getProperties());
    }
}
