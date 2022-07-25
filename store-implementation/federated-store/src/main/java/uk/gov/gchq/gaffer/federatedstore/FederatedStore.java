/*
 * Copyright 2017-2022 Crown Copyright
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.NoAccessUserPredicate;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraphWithHooks;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphAccess;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphId;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChainValidator;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.federatedstore.operation.IFederationOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedAggregateHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedFilterHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedTransformHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedValidateHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphWithHooksHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedChangeGraphAccessHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedChangeGraphIdHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllGraphIDHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllGraphInfoHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetSchemaHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetTraitsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedNoOutputHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOutputIterableHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedRemoveGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.schema.FederatedViewValidator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.IS_PUBLIC_ACCESS_ALLOWED_DEFAULT;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getCleanStrings;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedWrappedSchema;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getHardCodedDefaultMergeFunction;

/**
 * <p>
 * A Store that encapsulates a collection of sub-graphs and executes operations
 * against them and returns results as though it was a single graph.
 * <p>
 * To create a FederatedStore you need to initialise the store with a
 * graphId and (if graphId is not known by the {@link uk.gov.gchq.gaffer.store.library.GraphLibrary})
 * the {@link Schema} and {@link StoreProperties}.
 *
 * @see #initialise(String, Schema, StoreProperties)
 * @see Store
 * @see Graph
 */
public class FederatedStore extends Store {
    public static final String FEDERATED_STORE_PROCESSED = "FederatedStore.processed.";
    public static final String FED_STORE_GRAPH_ID_VALUE_NULL_OR_EMPTY = "FedStoreGraphId_value_null_or_empty";
    private static final Logger LOGGER = LoggerFactory.getLogger(Store.class);
    private static final List<Integer> ALL_IDS = new ArrayList<>();
    private final FederatedGraphStorage graphStorage = new FederatedGraphStorage();
    private final int id;
    private Set<String> customPropertiesAuths;
    private Boolean isPublicAccessAllowed = Boolean.valueOf(IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);
    private String adminConfiguredDefaultGraphIdsCSV;
    private BiFunction adminConfiguredDefaultMergeFunction;

    public FederatedStore() {
        Integer i = null;
        while (isNull(i) || ALL_IDS.contains(i)) {
            i = new Random().nextInt();
        }
        ALL_IDS.add(id = i);
    }

    /**
     * Initialise this FederatedStore with any sub-graphs defined within the
     * properties.
     *
     * @param graphId    the graphId to label this FederatedStore.
     * @param unused     unused
     * @param properties properties to initialise this FederatedStore with, can
     *                   contain details on graphs to add to scope.
     * @throws StoreException if no cache has been set
     */
    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, new Schema(), properties);
        customPropertiesAuths = getCustomPropertiesAuths();
        isPublicAccessAllowed = Boolean.valueOf(getProperties().getIsPublicAccessAllowed());
    }

    @Override
    public void setGraphLibrary(final GraphLibrary library) {
        super.setGraphLibrary(library);
        graphStorage.setGraphLibrary(library);
    }

    /**
     * Get this Store's {@link uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties}.
     *
     * @return the instance of {@link uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties},
     * this may contain details such as database connection details.
     */
    @Override
    public FederatedStoreProperties getProperties() {
        return (FederatedStoreProperties) super.getProperties();
    }

    /**
     * Adds graphs to the scope of FederatedStore.
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should add
     * graphs via the {@link AddGraph} operation.
     * public access will be ignored if the FederatedStore denies this action
     * at initialisation, will default to usual access with addingUserId and
     * graphAuths
     * </p>
     *
     * @param addingUserId the adding userId
     * @param graphs       the graph to add
     * @param isPublic     if this class should have public access.
     * @param graphAuths   the access auths for the graph being added
     * @throws StorageException if unable to put graph into storage
     */
    public void addGraphs(final Set<String> graphAuths,
                          final String addingUserId,
                          final boolean isPublic,
                          final GraphSerialisable... graphs) throws StorageException {
        addGraphs(graphAuths, addingUserId, isPublic, FederatedGraphStorage.DEFAULT_DISABLED_BY_DEFAULT, graphs);
    }

    /**
     * Adds graphs to the scope of FederatedStore.
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should add
     * graphs via the {@link AddGraph} operation.
     * public access will be ignored if the FederatedStore denies this action
     * at initialisation, will default to usual access with addingUserId and
     * graphAuths
     * </p>
     *
     * @param addingUserId      the adding userId
     * @param graphs            the graph to add
     * @param isPublic          if this class should have public access.
     * @param disabledByDefault true if the graph should be disabled by default - requiring the graphId to be provided in queries
     * @param graphAuths        the access auths for the graph being added
     * @throws StorageException if unable to put graph into storage
     */
    public void addGraphs(final Set<String> graphAuths,
                          final String addingUserId,
                          final boolean isPublic,
                          final boolean disabledByDefault,
                          final GraphSerialisable... graphs) throws StorageException {
        addGraphs(graphAuths, addingUserId, isPublic, disabledByDefault, null, null, graphs);
    }

    public void addGraphs(final Set<String> graphAuths,
                          final String addingUserId,
                          final boolean isPublic,
                          final boolean disabledByDefault,
                          final AccessPredicate readAccessPredicate,
                          final AccessPredicate writeAccessPredicate,
                          final GraphSerialisable... graphs) throws StorageException {
        final FederatedAccess access = new FederatedAccess(graphAuths, addingUserId, isPublicAccessAllowed && isPublic, disabledByDefault, readAccessPredicate, writeAccessPredicate);
        addGraphs(access, graphs);
    }

    public void addGraphs(final FederatedAccess access, final GraphSerialisable... graphs) throws StorageException {
        for (final GraphSerialisable graph : graphs) {
            _add(graph, access);
        }
    }

    /**
     * <p>
     * Removes graphs from the scope of FederatedStore.
     * </p>
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should remove
     * graphs via the {@link RemoveGraph} operation.
     * </p>
     *
     * @param graphId to be removed from scope
     * @param user    to match visibility against
     * @return success of removal
     */
    public boolean remove(final String graphId, final User user) {
        return remove(graphId, user, false);
    }

    public boolean remove(final String graphId, final User user, final boolean asAdmin) {
        return asAdmin
                ? graphStorage.remove(graphId, user, this.getProperties().getAdminAuth())
                : graphStorage.remove(graphId, user);
    }

    /**
     * @param user the visibility to use for getting graphIds
     * @return All the graphId(s) within scope of this FederatedStore and within
     * visibility for the given user.
     */
    public Collection<String> getAllGraphIds(final User user) {
        return getAllGraphIds(user, false);
    }

    public Collection<String> getAllGraphIds(final User user, final boolean userRequestingAdminUsage) {
        return userRequestingAdminUsage
                ? graphStorage.getAllIds(user, this.getProperties().getAdminAuth())
                : graphStorage.getAllIds(user);
    }

    /**
     * @return schema
     * @deprecated use {@link uk.gov.gchq.gaffer.store.Store#execute(Operation, Context)} with GetSchema Operation.
     */
    @Override
    @Deprecated
    public Schema getSchema() {
        return getSchema((Context) null);
    }

    /**
     * @param context context with User.
     * @return schema
     * @deprecated use {@link uk.gov.gchq.gaffer.store.Store#execute(Operation, Context)} with GetSchema Operation.
     */
    @Deprecated
    public Schema getSchema(final Context context) {
        return getSchema(getFederatedWrappedSchema(), context);
    }

    /**
     * @param operation operation with graphIds.
     * @param context   context with User.
     * @return schema
     * @deprecated use {@link uk.gov.gchq.gaffer.store.Store#execute(Operation, Context)} with GetSchema Operation.
     */
    @Deprecated
    public Schema getSchema(final FederatedOperation operation, final Context context) {
        return graphStorage.getSchema(operation, context);
    }

    /**
     * @return the {@link uk.gov.gchq.gaffer.store.StoreTrait}s for this store.
     * @see Store#getTraits()
     * @deprecated use {@link uk.gov.gchq.gaffer.store.Store#execute(Operation, Context)} with GetTraits Operation.
     */
    @Deprecated
    @Override
    public Set<StoreTrait> getTraits() {
        return StoreTrait.ALL_TRAITS;
    }

    /**
     * <p>
     * Gets a collection of graph objects within FederatedStore scope from the
     * given csv of graphIds, with visibility of the given user.
     * </p>
     * <p>
     * Graphs are returned once per operation, this does not allow an infinite loop of FederatedStores to occur.
     * </p>
     * <p>
     * if graphIdsCsv is null then all graph objects within FederatedStore
     * scope are returned.
     * </p>
     *
     * @param user        the users scope to get graphs for.
     * @param graphIdsCsv the csv of graphIds to get, null returns all graphs.
     * @param operation   the requesting operation, graphs are returned only once per operation.
     * @return the graph collection.
     */
    public Collection<Graph> getGraphs(final User user, final String graphIdsCsv, final IFederationOperation operation) {
        Collection<Graph> rtn = new ArrayList<>();
        if (nonNull(operation)) {
            String optionKey = getFedStoreProcessedKey();
            boolean isFedStoreIdPreexisting = addFedStoreId(operation, optionKey);
            if (isFedStoreIdPreexisting) {
                List<String> federatedStoreIds = operation.getOptions()
                        .entrySet()
                        .stream()
                        .filter(e -> e.getKey().startsWith(FEDERATED_STORE_PROCESSED))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
                String ln = System.lineSeparator();
                LOGGER.error("This operation has already been processed by this FederatedStore. " +
                        "This is a symptom of an infinite loop of FederatedStores and Proxies.{}" +
                        "This FederatedStore: {}{}" +
                        "All FederatedStore in this loop: {}", ln, this.getGraphId(), ln, federatedStoreIds.toString());
            } else if (isNull(graphIdsCsv)) {
                LOGGER.debug("getting default graphs because requested graphIdsCsv is null");
                rtn = getDefaultGraphs(user, operation);
            } else {
                String adminAuth = operation.isUserRequestingAdminUsage() ? this.getProperties().getAdminAuth() : null;
                rtn.addAll(graphStorage.get(user, getCleanStrings(graphIdsCsv), adminAuth));
            }
        } else {
            LOGGER.warn("getGraphs was requested with null Operation, this will return no graphs.");
        }
        return rtn;
    }

    private String getFedStoreProcessedKey() {
        return FEDERATED_STORE_PROCESSED + id;
    }

    private boolean addFedStoreId(final Operation operation, final String optionKey) {
        boolean rtn = false;
        if (nonNull(operation) && !isNullOrEmpty(optionKey)) {
            //Keep Order v
            boolean hasOperationPreexistingFedStoreId = !isNullOrEmpty(operation.getOption(optionKey, null)); //There is a difference between value null and key not found.
            //Keep Order ^
            boolean hasPayloadPreexistingFedStoreId = false;
            if (operation instanceof FederatedOperation) {
                //Check and Add FedStoreId to payload
                hasPayloadPreexistingFedStoreId = addFedStoreId(((FederatedOperation) operation).getUnClonedPayload(), optionKey);
            }

            //Add FedStoreId to current Operation.
            operation.addOption(optionKey, getFedStoreProcessedValue());
            rtn = hasOperationPreexistingFedStoreId || hasPayloadPreexistingFedStoreId;
        }
        return rtn;
    }

    public Map<String, Object> getAllGraphsAndAuths(final User user, final String graphIdsCsv) {
        return this.getAllGraphsAndAuths(user, graphIdsCsv, false);
    }

    public Map<String, Object> getAllGraphsAndAuths(final User user, final String graphIdsCsv, final boolean userRequestingAdminUsage) {
        List<String> graphIds = getCleanStrings(graphIdsCsv);
        return userRequestingAdminUsage
                ? graphStorage.getAllGraphsAndAccessAsAdmin(user, graphIds, this.getProperties().getAdminAuth())
                : graphStorage.getAllGraphsAndAccess(user, graphIds);
    }

    /**
     * The FederatedStore at time of initialisation, can set the auths required
     * to allow users to use custom {@link StoreProperties} outside the
     * scope of the {@link uk.gov.gchq.gaffer.store.library.GraphLibrary}.
     *
     * @param user the user needing validation for custom property usage.
     * @return boolean permission
     */
    public boolean isLimitedToLibraryProperties(final User user) {
        return isLimitedToLibraryProperties(user, false);
    }

    public boolean isLimitedToLibraryProperties(final User user, final boolean userRequestingAdminUsage) {

        boolean isAdmin = userRequestingAdminUsage && new AccessPredicate(new NoAccessUserPredicate()).test(user, this.getProperties().getAdminAuth());

        return !isAdmin
                && nonNull(this.customPropertiesAuths)
                && Collections.disjoint(user.getOpAuths(), this.customPropertiesAuths);
    }

    @Override
    protected Class<FederatedStoreProperties> getPropertiesClass() {
        return FederatedStoreProperties.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void addAdditionalOperationHandlers() {
        // Override the Operations that don't have an output
        getSupportedOperations()
                .stream()
                .filter(op -> !Output.class.isAssignableFrom(op)
                        && !AddElements.class.equals(op)
                        && !AddNamedOperation.class.equals(op)
                        && !AddNamedView.class.equals(op))
                .forEach(op -> addOperationHandler(op, new FederatedNoOutputHandler()));

        addOperationHandler(GetSchema.class, new FederatedGetSchemaHandler());

        addOperationHandler(Filter.class, new FederatedFilterHandler());
        addOperationHandler(Aggregate.class, new FederatedAggregateHandler());
        addOperationHandler(Transform.class, new FederatedTransformHandler());

        addOperationHandler(Validate.class, new FederatedValidateHandler());

        //FederationOperations
        addOperationHandler(GetAllGraphIds.class, new FederatedGetAllGraphIDHandler());
        addOperationHandler(AddGraph.class, new FederatedAddGraphHandler());
        addOperationHandler(AddGraphWithHooks.class, new FederatedAddGraphWithHooksHandler());
        addOperationHandler(RemoveGraph.class, new FederatedRemoveGraphHandler());

        addOperationHandler(GetAllGraphInfo.class, new FederatedGetAllGraphInfoHandler());
        addOperationHandler(ChangeGraphAccess.class, new FederatedChangeGraphAccessHandler());
        addOperationHandler(ChangeGraphId.class, new FederatedChangeGraphIdHandler());
        addOperationHandler(FederatedOperation.class, new FederatedOperationHandler());
        //TODO FS 1 re-add FedOpChain
    }

    @Override
    protected OperationChainValidator createOperationChainValidator() {
        return new FederatedOperationChainValidator(new FederatedViewValidator());
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return new FederatedOutputIterableHandler<>(/*default merge function*/);
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return new FederatedOutputIterableHandler<>(/*default merge function*/);
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new FederatedOutputIterableHandler<>(/*default merge function*/);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new FederatedNoOutputHandler<AddElements>();
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        return new FederatedGetTraitsHandler();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected void startCacheServiceLoader(final StoreProperties properties) {
        super.startCacheServiceLoader(properties);
        try {
            graphStorage.startCacheServiceLoader();
        } catch (final StorageException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Set<String> getCustomPropertiesAuths() {
        final String value = getProperties().getCustomPropsValue();
        return (isNullOrEmpty(value)) ? null : Sets.newHashSet(getCleanStrings(value));
    }

    private void _add(final GraphSerialisable newGraph, final FederatedAccess access) throws StorageException {
        graphStorage.put(newGraph, access);
    }

    public boolean changeGraphAccess(final User requestingUser, final String graphId, final FederatedAccess federatedAccess, final boolean isAdmin) throws StorageException {
        return isAdmin
                ? graphStorage.changeGraphAccess(graphId, federatedAccess, requestingUser, this.getProperties().getAdminAuth())
                : graphStorage.changeGraphAccess(graphId, federatedAccess, requestingUser);
    }

    public boolean changeGraphId(final User requestingUser, final String graphId, final String newGraphId, final boolean isAdmin) throws StorageException {
        return isAdmin
                ? graphStorage.changeGraphId(graphId, newGraphId, requestingUser, this.getProperties().getAdminAuth())
                : graphStorage.changeGraphId(graphId, newGraphId, requestingUser);
    }

    public String getAdminConfiguredDefaultGraphIdsCSV() {
        return adminConfiguredDefaultGraphIdsCSV;
    }

    /**
     * Sets the configurable default graphIds once only. To change the adminConfiguredDefaultGraphIdsCSV it would require to turning off, update config, turning back on.
     *
     * @param adminConfiguredDefaultGraphIdsCSV graphID CSV to use.
     * @return This Store.
     */
    public FederatedStore setAdminConfiguredDefaultGraphIdsCSV(final String adminConfiguredDefaultGraphIdsCSV) {
        if (nonNull(this.adminConfiguredDefaultGraphIdsCSV)) {
            LOGGER.error("Attempting to change adminConfiguredDefaultGraphIdsCSV. To change adminConfiguredDefaultGraphIdsCSV it would require to turning off, update config, turn back on. Therefore ignoring the value: {}", adminConfiguredDefaultGraphIdsCSV);
        } else {
            this.adminConfiguredDefaultGraphIdsCSV = adminConfiguredDefaultGraphIdsCSV;
        }
        return this;
    }

    public Collection<Graph> getDefaultGraphs(final User user, final IFederationOperation operation) {

        boolean isAdminRequestingOverridingDefaultGraphs =
                operation.isUserRequestingAdminUsage()
                        && (operation instanceof FederatedOperation)
                        && ((FederatedOperation) operation).isUserRequestingDefaultGraphsOverride();

        //TODO FS Test does this preserve get graph.disabledByDefault?
        if (isNull(adminConfiguredDefaultGraphIdsCSV) || isAdminRequestingOverridingDefaultGraphs) {
            return graphStorage.get(user, null, (operation.isUserRequestingAdminUsage() ? getProperties().getAdminAuth() : null));
        } else {
            //This operation has already been processes once, by this store.
            String fedStoreProcessedKey = getFedStoreProcessedKey();
            operation.addOption(fedStoreProcessedKey, null); // value is null, but key is still found.
            Collection<Graph> graphs = getGraphs(user, adminConfiguredDefaultGraphIdsCSV, operation);
            //put it back
            operation.addOption(fedStoreProcessedKey, getFedStoreProcessedValue());
            return graphs;
        }
    }

    private String getFedStoreProcessedValue() {
        return isNullOrEmpty(getGraphId()) ? FED_STORE_GRAPH_ID_VALUE_NULL_OR_EMPTY : getGraphId();
    }

    public FederatedStore setAdminConfiguredDefaultMergeFunction(final BiFunction adminConfiguredDefaultMergeFunction) {
        this.adminConfiguredDefaultMergeFunction = adminConfiguredDefaultMergeFunction;
        return this;
    }

    public BiFunction getDefaultMergeFunction() {
        return isNull(adminConfiguredDefaultMergeFunction)
                ? getHardCodedDefaultMergeFunction()
                : adminConfiguredDefaultMergeFunction;
    }
}
