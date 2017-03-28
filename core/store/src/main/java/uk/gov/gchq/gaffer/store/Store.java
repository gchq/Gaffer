/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.Deduplicate;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdgesBySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElementsBySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntitiesBySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEntities;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.CountHandler;
import uk.gov.gchq.gaffer.store.operation.handler.DeduplicateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.LimitHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ValidateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.GetExportsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.ExportToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.GetSetExportHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetAllJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobResultsHandler;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.optimiser.CoreOperationChainOptimiser;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A <code>Store</code> backs a Graph and is responsible for storing the {@link uk.gov.gchq.gaffer.data.element.Element}s and
 * handling {@link Operation}s.
 * {@link Operation}s and their corresponding {@link OperationHandler}s are registered in a map and used to handle
 * provided operations - allowing different store implementations to handle the same operations in their own store specific way.
 * Optional functionality can be added to store implementations defined by the {@link uk.gov.gchq.gaffer.store.StoreTrait}s.
 */
public abstract class Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(Store.class);

    /**
     * The schema - contains the type of {@link uk.gov.gchq.gaffer.data.element.Element}s to be stored and how to aggregate the elements.
     */
    private Schema schema;

    /**
     * The store properties - contains specific configuration information for the store - such as database connection strings.
     */
    private StoreProperties properties;

    private final Map<Class<? extends Operation>, OperationHandler> operationHandlers = new LinkedHashMap<>();
    private final List<OperationChainOptimiser> opChainOptimisers = new ArrayList<>();
    private SchemaOptimiser schemaOptimiser;
    private ViewValidator viewValidator;

    private JobTracker jobTracker;

    public Store() {
        opChainOptimisers.add(new CoreOperationChainOptimiser(this));
        this.viewValidator = new ViewValidator();
        this.schemaOptimiser = new SchemaOptimiser();
    }

    public void initialise(final Schema schema, final StoreProperties properties) throws StoreException {
        this.schema = schema;
        this.properties = properties;
        this.jobTracker = createJobTracker(properties);

        addOpHandlers();
        optimiseSchema();
        validateSchemas();
    }

    protected JobTracker createJobTracker(final StoreProperties properties) {
        final String jobTrackerClass = properties.getJobTrackerClass();
        if (null != jobTrackerClass) {
            try {
                final JobTracker newJobTracker = Class.forName(jobTrackerClass).asSubclass(JobTracker.class).newInstance();
                newJobTracker.initialise(properties.getJobTrackerConfigPath());
                return newJobTracker;
            } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not create job tracker with class: " + jobTrackerClass, e);
            }
        }

        return null;
    }

    /**
     * Returns true if the Store can handle the provided trait and false if it cannot.
     *
     * @param storeTrait the Class of the Processor to be checked.
     * @return true if the Processor can be handled and false if it cannot.
     */
    public boolean hasTrait(final StoreTrait storeTrait) {
        final Set<StoreTrait> traits = getTraits();
        return null != traits && traits.contains(storeTrait);
    }

    /**
     * Returns the {@link uk.gov.gchq.gaffer.store.StoreTrait}s for this store. Most stores should support FILTERING.
     * <p>
     * If you use Operation.validateFilter(Element) in you handlers, it will deal with the filtering for you.
     *
     * @return the {@link uk.gov.gchq.gaffer.store.StoreTrait}s for this store.
     */
    public abstract Set<StoreTrait> getTraits();

    /**
     * @return true if the store requires validation, so it requires Validatable operations to have a validation step.
     */
    public abstract boolean isValidationRequired();

    /**
     * Executes a given operation and returns the result.
     *
     * @param operation   the operation to execute.
     * @param <OPERATION> the operation type
     * @param user        the user executing the operation
     * @param <OUTPUT>    the output type.
     * @return the result from the operation
     * @throws OperationException thrown by the operation handler if the operation fails.
     */
    public <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT execute(
            final OPERATION operation, final User user) throws OperationException {
        return execute(new OperationChain<>(operation), user);
    }

    /**
     * Executes a given operation chain and returns the result.
     *
     * @param operationChain the operation chain to execute.
     * @param user           the user executing the operation chain
     * @param <OUTPUT>       the output type of the operation.
     * @return the result of executing the operation.
     * @throws OperationException thrown by an operation handler if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> operationChain, final User user) throws OperationException {
        final Context context = createContext(user);
        addOrUpdateJobDetail(operationChain, context, null, JobStatus.RUNNING);
        try {
            final OUTPUT result = _execute(operationChain, context);
            addOrUpdateJobDetail(operationChain, context, null, JobStatus.FINISHED);
            return result;
        } catch (final Throwable t) {
            addOrUpdateJobDetail(operationChain, context, t.getMessage(), JobStatus.FAILED);
            throw t;
        }
    }

    /**
     * Executes a given operation chain job and returns the job detail.
     *
     * @param operationChain the operation chain to execute.
     * @param user           the user executing the job
     * @return the job detail
     * @throws OperationException thrown if jobs are not configured.
     */
    public JobDetail executeJob(final OperationChain<?> operationChain, final User user) throws OperationException {
        if (null == jobTracker) {
            throw new OperationException("Running jobs has not configured.");
        }

        final Context context = createContext(user);

        if (isSupported(ExportToGafferResultCache.class)) {
            boolean hasExport = false;
            for (final Operation operation : operationChain.getOperations()) {
                if (operation instanceof ExportToGafferResultCache) {
                    hasExport = true;
                    break;
                }
            }
            if (!hasExport) {
                operationChain.getOperations().add(new ExportToGafferResultCache());
            }
        }

        final JobDetail initialJobDetail = addOrUpdateJobDetail(operationChain, context, null, JobStatus.RUNNING);
        new Thread(() -> {
            try {
                _execute(operationChain, context);
                addOrUpdateJobDetail(operationChain, context, null, JobStatus.FINISHED);
            } catch (final Throwable t) {
                LOGGER.warn("Operation chain job failed to execute", t);
                addOrUpdateJobDetail(operationChain, context, t.getMessage(), JobStatus.FAILED);
            }
        }).start();

        return initialJobDetail;
    }

    public <OUTPUT> OUTPUT _execute(final OperationChain<OUTPUT> operationChain, final Context context) throws OperationException {
        final OperationChain<OUTPUT> optimisedOperationChain = prepareOperationChain(operationChain, context);
        return handleOperationChain(optimisedOperationChain, context);
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
        return operationHandler != null;
    }

    /**
     * @return a collection of all the supported {@link Operation}s.
     */
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return operationHandlers.keySet();
    }

    /**
     * Ensures all identifier and property values are populated on an element by triggering getters on the element for
     * all identifier and properties in the {@link Schema} forcing a lazy element to load all of its values.
     *
     * @param lazyElement the lazy element
     * @return the fully populated unwrapped element
     */
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "Getters are called to trigger the loading data")
    public Element populateElement(final Element lazyElement) {
        final SchemaElementDefinition elementDefinition = getSchema().getElement(lazyElement.getGroup());
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

    /**
     * Get this Store's {@link Schema}.
     *
     * @return the instance of {@link Schema} used for describing the type of
     * {@link uk.gov.gchq.gaffer.data.element.Element}s to be stored and how to aggregate the elements.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Get this Store's {@link uk.gov.gchq.gaffer.store.StoreProperties}.
     *
     * @return the instance of {@link uk.gov.gchq.gaffer.store.StoreProperties}, this may contain details such as database connection details.
     */
    public StoreProperties getProperties() {
        return properties;
    }

    public void optimiseSchema() {
        schema = schemaOptimiser.optimise(schema, hasTrait(StoreTrait.ORDERED));
    }

    public void validateSchemas() {
        boolean valid = schema.validate();

        final HashMap<String, SchemaElementDefinition> schemaElements = new HashMap<>();
        schemaElements.putAll(getSchema().getEdges());
        schemaElements.putAll(getSchema().getEntities());
        for (final Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry : schemaElements.entrySet()) {
            for (final String propertyName : schemaElementDefinitionEntry.getValue().getProperties()) {
                Class propertyClass = schemaElementDefinitionEntry.getValue().getPropertyClass(propertyName);
                Serialisation serialisation = schemaElementDefinitionEntry.getValue().getPropertyTypeDef(propertyName).getSerialiser();
                if (null == serialisation) {
                    valid = false;
                    LOGGER.error("Could not find a serialiser for property '" + propertyName + "' in the group '" + schemaElementDefinitionEntry.getKey() + "'.");
                } else if (!serialisation.canHandle(propertyClass)) {
                    valid = false;
                    LOGGER.error("Schema serialiser (" + serialisation.getClass().getName() + ") for property '" + propertyName + "' in the group '" + schemaElementDefinitionEntry.getKey() + "' cannot handle property found in the schema");
                }
            }
        }
        if (!valid) {
            throw new SchemaException("Schema is not valid. Check the logs for more information.");
        }
    }

    protected <OUTPUT> OperationChain<OUTPUT> prepareOperationChain(final OperationChain<OUTPUT> operationChain, final Context context) {
        validateOperationChain(operationChain, context.getUser());

        OperationChain<OUTPUT> optimisedOperationChain = operationChain;
        for (final OperationChainOptimiser opChainOptimiser : opChainOptimisers) {
            optimisedOperationChain = opChainOptimiser.optimise(optimisedOperationChain);
        }
        return optimisedOperationChain;
    }

    protected void validateOperationChain(
            final OperationChain<?> operationChain, final User user) {
        if (operationChain.getOperations().isEmpty()) {
            throw new IllegalArgumentException("Operation chain contains no operations");
        }

        for (final Operation<?, ?> op : operationChain.getOperations()) {
            if (!viewValidator.validate(op.getView(), schema, hasTrait(StoreTrait.ORDERED))) {
                throw new SchemaException("View for operation "
                        + op.getClass().getName()
                        + " is not valid. See the logs for more information.");
            }
        }
    }

    protected void setSchemaOptimiser(final SchemaOptimiser schemaOptimiser) {
        this.schemaOptimiser = schemaOptimiser;
    }

    protected void setViewValidator(final ViewValidator viewValidator) {
        this.viewValidator = viewValidator;
    }

    protected void addOperationChainOptimisers(final List<OperationChainOptimiser> newOpChainOptimisers) {
        opChainOptimisers.addAll(newOpChainOptimisers);
    }

    protected Context createContext(final User user) {
        return new Context(user);
    }

    /**
     * Any additional operations that a store can handle should be registered in this method by calling addOperationHandler(...)
     */
    protected abstract void addAdditionalOperationHandlers();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements}. All Stores must implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements}
     */
    protected abstract OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetAllElements}. All Stores must implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetAllElements}
     */
    protected abstract OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds}
     */
    protected abstract OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler();

    /**
     * Get this Stores implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.add.AddElements}. All Stores must implement this.
     *
     * @return the implementation of the handler for {@link uk.gov.gchq.gaffer.operation.impl.add.AddElements}
     */
    protected abstract OperationHandler<? extends AddElements, Void> getAddElementsHandler();

    /**
     * Should deal with any unhandled operations, could simply throw an {@link UnsupportedOperationException}.
     *
     * @param <OUTPUT>  the operation output type
     * @param operation the operation that does not have a registered handler.
     * @param context   operation execution context
     * @return the result of the operation.
     */
    protected abstract <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context);

    protected final void addOperationHandler(final Class<? extends Operation> opClass, final OperationHandler handler) {
        operationHandlers.put(opClass, handler);
    }

    protected final <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OperationHandler<OPERATION, OUTPUT> getOperationHandler(final Class<? extends Operation> opClass) {
        return operationHandlers.get(opClass);
    }

    protected <OUTPUT> OUTPUT handleOperationChain(
            final OperationChain<OUTPUT> operationChain, final Context context) throws
            OperationException {
        Object result = null;
        for (final Operation op : operationChain.getOperations()) {
            updateOperationInput(op, result);
            result = handleOperation(op, context);
        }

        return (OUTPUT) result;
    }

    private JobDetail addOrUpdateJobDetail(final OperationChain<?> operationChain, final Context context, final String msg, final JobStatus jobStatus) {
        final JobDetail newJobDetail = new JobDetail(context.getJobId(), context.getUser().getUserId(), operationChain, jobStatus, msg);
        if (null != jobTracker) {
            final JobDetail oldJobDetail = jobTracker.getJob(newJobDetail.getJobId(), context.getUser());
            if (null == oldJobDetail) {
                jobTracker.addOrUpdateJob(newJobDetail, context.getUser());
            } else {
                jobTracker.addOrUpdateJob(new JobDetail(oldJobDetail, newJobDetail), context.getUser());
            }
        }
        return newJobDetail;
    }

    protected <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT handleOperation(final OPERATION operation, final Context context) throws
            OperationException {
        final OperationHandler<OPERATION, OUTPUT> handler = getOperationHandler(operation.getClass());
        OUTPUT result;
        if (null != handler) {
            result = handler.doOperation(operation, context, this);
        } else {
            result = doUnhandledOperation(operation, context);
        }

        return result;
    }

    protected void updateOperationInput(final Operation op,
                                        final Object result) {
        if (null != result && null == op.getInput()) {
            try {
                op.setInput(result);
            } catch (final ClassCastException e) {
                throw new UnsupportedOperationException("Operation chain is not compatible. "
                        + op.getClass().getName() + " cannot take " + result.getClass().getName() + " as an input", e);
            } catch (final IllegalArgumentException e) {
                // this is due to get all element operations not allowing seeds.
                // skip the error and just don't set the seeds
            }
        }
    }

    private void addOpHandlers() {
        addCoreOpHandlers();
        addAdditionalOperationHandlers();
        addConfiguredOperationHandlers();
    }

    private void addCoreOpHandlers() {
        // Add elements
        addOperationHandler(AddElements.class, getAddElementsHandler());

        // Get Elements
        addOperationHandler(GetElements.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetEntities.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetEdges.class, (OperationHandler) getGetElementsHandler());

        // Get Adjacent
        addOperationHandler(GetAdjacentEntitySeeds.class, (OperationHandler) getAdjacentEntitySeedsHandler());

        // Get All Elements
        addOperationHandler(GetAllElements.class, (OperationHandler) getGetAllElementsHandler());
        addOperationHandler(GetAllEntities.class, (OperationHandler) getGetAllElementsHandler());
        addOperationHandler(GetAllEdges.class, (OperationHandler) getGetAllElementsHandler());

        // Deprecated Get operations
        addOperationHandler(GetEdgesBySeed.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetElementsBySeed.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetEntitiesBySeed.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetRelatedEdges.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetRelatedElements.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetRelatedEntities.class, (OperationHandler) getGetElementsHandler());

        // Export
        addOperationHandler(ExportToSet.class, new ExportToSetHandler());
        addOperationHandler(GetSetExport.class, new GetSetExportHandler());
        addOperationHandler(GetExports.class, new GetExportsHandler());

        // Jobs
        addOperationHandler(GetJobDetails.class, new GetJobDetailsHandler());
        addOperationHandler(GetAllJobDetails.class, new GetAllJobDetailsHandler());
        addOperationHandler(GetJobResults.class, new GetJobResultsHandler());

        // Other
        addOperationHandler(GenerateElements.class, new GenerateElementsHandler<>());
        addOperationHandler(GenerateObjects.class, new GenerateObjectsHandler<>());
        addOperationHandler(Validate.class, new ValidateHandler());
        addOperationHandler(Deduplicate.class, new DeduplicateHandler());
        addOperationHandler(Count.class, new CountHandler());
        addOperationHandler(CountGroups.class, new CountGroupsHandler());
        addOperationHandler(Limit.class, new LimitHandler());
    }

    private void addConfiguredOperationHandlers() {
        final OperationDeclarations declarations = getProperties().getOperationDeclarations();
        if (null != declarations) {
            for (final OperationDeclaration definition : declarations.getOperations()) {
                addOperationHandler(definition.getOperation(), definition.getHandler());
            }
        }
    }
}
