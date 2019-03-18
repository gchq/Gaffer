/*
 * Copyright 2016-2019 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.ExecutorService;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
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
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
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
import uk.gov.gchq.gaffer.store.operation.OperationUtil;
import uk.gov.gchq.gaffer.store.operation.OperationValidation;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.CountHandler;
import uk.gov.gchq.gaffer.store.operation.handler.DiscardOutputHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ForEachHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetVariableHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetVariablesHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetWalksHandler;
import uk.gov.gchq.gaffer.store.operation.handler.IfHandler;
import uk.gov.gchq.gaffer.store.operation.handler.LimitHandler;
import uk.gov.gchq.gaffer.store.operation.handler.MapHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ReduceHandler;
import uk.gov.gchq.gaffer.store.operation.handler.SetVariableHandler;
import uk.gov.gchq.gaffer.store.operation.handler.WhileHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MaxHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MinHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.SortHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.GetExportsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.ExportToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.GetSetExportHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetAllJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobResultsHandler;
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
import uk.gov.gchq.gaffer.store.util.Config;
import uk.gov.gchq.gaffer.store.util.Hook;
import uk.gov.gchq.gaffer.store.util.Request;
import uk.gov.gchq.gaffer.store.util.Result;
import uk.gov.gchq.gaffer.user.User;

import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@code Store} is responsible for storing Objects and
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
    /**
     * The Config - contains specific information about the configuration of
     * the Store, and can be used to provide details about a Graph, for
     * example, and util methods for these.  See GraphConfig.
     */
    private Config config;

    private JobTracker jobTracker;

    public Store() {
    }

    public static Store createStore(final Config config) {
        return createStore(config.getId(), config.getProperties());
    }

    public static Store createStore(final String id,
                                    final Properties storeProperties) {
        return createStore(id, StoreProperties.loadStoreProperties(storeProperties));
    }

    public static Store createStore(final Config config,
                                    final Properties storeProperties) {
        return createStore(config.getId(), config, StoreProperties.loadStoreProperties(storeProperties));
    }

    public static Store createStore(final String id,
                                    final StoreProperties storeProperties) {
        return createStore(id, new Config(), storeProperties);
    }

    public static Store createStore(final String id,
                                    final Config config,
                                    final StoreProperties storeProperties) {
        if (null == storeProperties) {
            throw new IllegalArgumentException("Store properties are required to create a store. id: " + id);
        }

        final String storeClass = storeProperties.getStoreClass();
        if (null == storeClass) {
            throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS + ", id: " + id);
        }

        final Store newStore;
        try {
            newStore = Class.forName(storeClass)
                    .asSubclass(Store.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create store of type: " + storeClass, e);
        }

        try {
            newStore.initialise(id, config, storeProperties);
        } catch (final StoreException e){
            throw new IllegalArgumentException("Could not create Store, ", e);
        }

        return newStore;
    }

    public void initialise(final String id, final StoreProperties properties) throws StoreException {
        if (null == this.config) {
            config = new Config();
        }

        config.setId(id);
        config.setProperties(properties);
        initialise(config);
    }

    public void initialise(final Config config) throws StoreException {
        LOGGER.debug("Initialising {}", getClass().getSimpleName());
        if (null == config) {
            throw new IllegalArgumentException("a Config is required");
        }
        if (null == config.getId()) {
            throw new IllegalArgumentException("id is required within the " +
                    "Config");
        }
        if (null == config.getProperties()) {
            throw new IllegalArgumentException("Store properties are required" +
                    " to create a store. id: " + config.getId());
        }

        this.config = config;

        config.updateJsonSerialiser();

        startCacheServiceLoader(config.getProperties());
        this.jobTracker = createJobTracker();

        addOpHandlers();
        addExecutorService(config.getProperties());

        initialise(config.getId(), config, config.getProperties());
    }

    public void initialise(final String id, final Config config,
                           final StoreProperties properties) throws StoreException{
        if (null == config) {
            this.config = new Config();
            config.setId(id);
        }
        if (config.getId() != id) {
            throw new IllegalArgumentException("Supplied id and Config " +
                    "id do not match");
        }
        config.setProperties(properties);
        initialise(config);
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

    public <O> O execute(final Operation operation,
                         final Context context) throws OperationException {
        return (O) execute(new Request(operation, context)).getResult();
    }

    public <O> O execute(final Operation operation,
                         final User user) throws OperationException {
        return (O) execute(new Request(operation, new Context(user))).getResult();
    }

    /**
     * Executes a given operation and returns the result.
     *
     * @param request the request to execute.
     * @param <O>     the output type of the operation
     * @return the result of executing the operation
     * @throws OperationException thrown by the operation handler if the
     *                            operation fails.
     */
    public <O> Result<O> execute(final Request request) {
        if (null == request) {
            throw new IllegalArgumentException("A request is required");
        }

        if (null == request.getContext()) {
            throw new IllegalArgumentException("A context is required");
        }

        request.setConfig(config);
        request.getContext().setOriginalOperation(request.getOperation());
        final Request clonedRequest = request.fullClone();
        final Operation operation = clonedRequest.getOperation();
        final Context context = clonedRequest.getContext();

        addOrUpdateJobDetail(operation, context, null, JobStatus.RUNNING);
        O result = null;
        try {
            for (final Hook graphHook : getConfig().getHooks()) {
                graphHook.preExecute(clonedRequest);
            }
            result = (O) handleOperation(operation, context);
            for (final Hook graphHook : getConfig().getHooks()) {
                result = graphHook.postExecute(result,
                        clonedRequest);
            }
            addOrUpdateJobDetail(operation, context, null, JobStatus.FINISHED);
        } catch (final Exception e) {
            for (final Hook graphHook : getConfig().getHooks()) {
                try {
                    result = graphHook.onFailure(result,
                            clonedRequest, e);
                } catch (final Exception graphHookE) {
                    LOGGER.warn("Error in graphHook " + graphHook.getClass().getSimpleName() + ": " + graphHookE.getMessage(), graphHookE);
                }
            }
        } catch (final Throwable t) {
            addOrUpdateJobDetail(operation, context, t.getMessage(), JobStatus.FAILED);
            throw t;
        }
        return new Result(result, clonedRequest.getContext());
    }

    protected <O> O execute(final OperationChain<O> operation, final Context context) throws OperationException {
        return (O) execute(new Request((Operation) operation, context));
    }

    public JobDetail executeJob(final Operation operation,
                                final Context context) throws OperationException {
        return executeJob(new Request(operation, context)).getResult();
    }

    public JobDetail executeJob(final Operation operation,
                                final User user) throws OperationException {
        return executeJob(new Request(operation, new Context(user))).getResult();
    }

    /**
     * Executes a given operation job and returns the job detail.
     *
     * @param request the request to execute.
     * @return the job detail
     * @throws OperationException thrown if jobs are not configured.
     */
    public Result<JobDetail> executeJob(final Request request) throws OperationException {
        if (null == jobTracker) {
            throw new OperationException("Running jobs has not configured.");
        }
        if (null == request) {
            throw new IllegalArgumentException("A request is required");
        }

        if (null == request.getContext()) {
            throw new IllegalArgumentException("A context is required");
        }

        request.setConfig(config);
        request.getContext().setOriginalOperation(request.getOperation());
        final Request clonedRequest = request.fullClone();
        final Operation operation = clonedRequest.getOperation();
        final Context context = clonedRequest.getContext();

        if (isSupported(ExportToGafferResultCache.class)) {
            boolean hasExport = false;
            if (operation instanceof Operations) {

                for (final Operation op :
                        ((Operations<?>) operation).getOperations()) {
                    if (op instanceof ExportToGafferResultCache) {
                        hasExport = true;
                        break;
                    }
                }
                if (!hasExport) {
                    ((OperationChain) operation).getOperations()
                            .add(new ExportToGafferResultCache());
                }
            }
        }

        final JobDetail initialJobDetail = addOrUpdateJobDetail(operation, context, null, JobStatus.RUNNING);
        runAsync(() -> {
            try {
                handleOperation(operation, context);
                addOrUpdateJobDetail(operation, context, null, JobStatus.FINISHED);
            } catch (final Error e) {
                addOrUpdateJobDetail(operation, context, e.getMessage(), JobStatus.FAILED);
                throw e;
            } catch (final Exception e) {
                LOGGER.warn("Operation chain job failed to execute", e);
                addOrUpdateJobDetail(operation, context, e.getMessage(), JobStatus.FAILED);
            }
        });

        return new Result(initialJobDetail, context);
    }

    protected JobDetail executeJob(final OperationChain<?> operationChain,
                                   final Context context) throws OperationException {
        return executeJob(new Request((Operation) operationChain, context)).getResult();
    }

    public void runAsync(final Runnable runnable) {
        getExecutorService().execute(runnable);
    }

    protected ScheduledExecutorService getExecutorService() {
        return (null != ExecutorService.getService() && ExecutorService.isEnabled()) ?
                ExecutorService.getService() : null;
    }

    public JobTracker getJobTracker() {
        return jobTracker;
    }

    /**
     * @param operationClass the operation class to check
     * @return true if the provided operation is supported.
     */
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        final OperationHandler operationHandler =
                config.getOperationHandlers().get(operationClass);
        return null != operationHandler;
    }

    /**
     * @return a collection of all the supported {@link Operation}s.
     */
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return config.getOperationHandlers().keySet();
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

    public String getId() {
        return config.getId();
    }

    protected JobTracker createJobTracker() {
        if (config.getProperties().getJobTrackerEnabled()) {
            return new JobTracker();
        }
        return null;
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
        return new OperationChainHandler<>();
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

    private JobDetail addOrUpdateJobDetail(final Operation operation,
                                           final Context context, final String msg, final JobStatus jobStatus) {
        final JobDetail newJobDetail = new JobDetail(context.getJobId(), context
                .getUser()
                .getUserId(), OperationChain.wrap(operation), jobStatus, msg);
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
        final OperationHandler<Operation> handler =
                config.getOperationHandler(operation.getClass());
        Object result;
        try {
            if (null != handler) {
                if (handler instanceof OperationValidation) {
                    ((OperationValidation) handler).prepareOperation(operation,
                            context, this);
                }
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

    private void addExecutorService(final StoreProperties properties) {
        ExecutorService.initialise(properties.getJobExecutorThreadCount());
    }

    private void addOpHandlers() {
        addCoreOpHandlers();
        addAdditionalOperationHandlers();
        addConfiguredOperationHandlers();
    }

    private void addCoreOpHandlers() {
        // Add elements
        config.addOperationHandler(AddElements.class, getAddElementsHandler());

        // Get Elements
        config.addOperationHandler(GetElements.class, (OperationHandler) getGetElementsHandler());

        // Get Adjacent
        config.addOperationHandler(GetAdjacentIds.class, (OperationHandler) getAdjacentIdsHandler());

        // Get All Elements
        config.addOperationHandler(GetAllElements.class, (OperationHandler) getGetAllElementsHandler());

        // Export
        config.addOperationHandler(ExportToSet.class, new ExportToSetHandler());
        config.addOperationHandler(GetSetExport.class, new GetSetExportHandler());
        config.addOperationHandler(GetExports.class, new GetExportsHandler());

        // Jobs
        if (null != getJobTracker()) {
            config.addOperationHandler(GetJobDetails.class, new GetJobDetailsHandler());
            config.addOperationHandler(GetAllJobDetails.class, new GetAllJobDetailsHandler());
            config.addOperationHandler(GetJobResults.class, new GetJobResultsHandler());
        }

        // Output
        config.addOperationHandler(ToArray.class, new ToArrayHandler<>());
        config.addOperationHandler(ToEntitySeeds.class, new ToEntitySeedsHandler());
        config.addOperationHandler(ToList.class, new ToListHandler<>());
        config.addOperationHandler(ToMap.class, new ToMapHandler());
        config.addOperationHandler(ToCsv.class, new ToCsvHandler());
        config.addOperationHandler(ToSet.class, new ToSetHandler<>());
        config.addOperationHandler(ToStream.class, new ToStreamHandler<>());
        config.addOperationHandler(ToVertices.class, new ToVerticesHandler());

        if (null != CacheServiceLoader.getService()) {
            // Named operation
            config.addOperationHandler(NamedOperation.class, new NamedOperationHandler());
            config.addOperationHandler(AddNamedOperation.class, new AddNamedOperationHandler());
            config.addOperationHandler(GetAllNamedOperations.class, new GetAllNamedOperationsHandler());
            config.addOperationHandler(DeleteNamedOperation.class, new DeleteNamedOperationHandler());

            // Named view
            config.addOperationHandler(AddNamedView.class, new AddNamedViewHandler());
            config.addOperationHandler(GetAllNamedViews.class, new GetAllNamedViewsHandler());
            config.addOperationHandler(DeleteNamedView.class, new DeleteNamedViewHandler());
        }

        // ElementComparison
        config.addOperationHandler(Max.class, new MaxHandler());
        config.addOperationHandler(Min.class, new MinHandler());
        config.addOperationHandler(Sort.class, new SortHandler());

        // OperationChain
        config.addOperationHandler(OperationChain.class, getOperationChainHandler());
        config.addOperationHandler(OperationChainDAO.class, getOperationChainHandler());

        // Walk tracking
        config.addOperationHandler(GetWalks.class, new GetWalksHandler());

        // Other
        config.addOperationHandler(GenerateElements.class, new GenerateElementsHandler<>());
        config.addOperationHandler(GenerateObjects.class, new GenerateObjectsHandler<>());
        config.addOperationHandler(Count.class, new CountHandler());
        config.addOperationHandler(CountGroups.class, new CountGroupsHandler());
        config.addOperationHandler(Limit.class, new LimitHandler());
        config.addOperationHandler(DiscardOutput.class, new DiscardOutputHandler());
        config.addOperationHandler(uk.gov.gchq.gaffer.operation.impl.Map.class, new MapHandler());
        config.addOperationHandler(If.class, new IfHandler());
        config.addOperationHandler(While.class, new WhileHandler());
        config.addOperationHandler(ForEach.class, new ForEachHandler());
        config.addOperationHandler(ToSingletonList.class, new ToSingletonListHandler());
        config.addOperationHandler(Reduce.class, new ReduceHandler());

        // Context variables
        config.addOperationHandler(SetVariable.class, new SetVariableHandler());
        config.addOperationHandler(GetVariable.class, new GetVariableHandler());
        config.addOperationHandler(GetVariables.class, new GetVariablesHandler());
    }

    private void addConfiguredOperationHandlers() {
        final OperationDeclarations declarations = config.getProperties().getOperationDeclarations();
        if (null != declarations) {
            for (final OperationDeclaration definition : declarations.getOperations()) {
                config.addOperationHandler(definition.getOperation(), definition.getHandler());
            }
        }
    }

    protected void startCacheServiceLoader(final StoreProperties properties) {
        CacheServiceLoader.initialise(properties.getProperties());
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(final Config config) {
        this.config = config;
    }
}
