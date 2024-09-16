/*
 * Copyright 2017-2024 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.NoAccessUserPredicate;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
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
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraphAndDeleteAllData;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedDelegateToHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphWithHooksHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedChangeGraphAccessHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedChangeGraphIdHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllGraphIDHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllGraphInfoHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedJoinHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedNoOutputHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOutputHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOutputIterableHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedRemoveGraphAndDeleteAllDataHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedRemoveGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedWhileHandler;
import uk.gov.gchq.gaffer.federatedstore.schema.FederatedViewValidator;
import uk.gov.gchq.gaffer.federatedstore.util.ApplyViewToElementsFunction;
import uk.gov.gchq.gaffer.federatedstore.util.MergeSchema;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ValidateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.AggregateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.FilterHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.TransformHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionIntersect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.cache.CacheServiceLoader.DEFAULT_SERVICE_NAME;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreCacheTransient.FEDERATED_STORE_CACHE_SERVICE_NAME;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.FEDERATED_STORE_SYSTEM_USER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.CACHE_SERVICE_CLASS_DEFAULT;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.IS_PUBLIC_ACCESS_ALLOWED_DEFAULT;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.getCacheServiceFederatedStoreSuffix;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getCleanStrings;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.loadStoreConfiguredGraphIdsListFrom;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.loadStoreConfiguredMergeFunctionMapFrom;

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
 * @see uk.gov.gchq.gaffer.graph.Graph
 */
public class FederatedStore extends Store {
    public static final String FEDERATED_STORE_PROCESSED = "FederatedStore.processed.";
    public static final String FED_STORE_GRAPH_ID_VALUE_NULL_OR_EMPTY = "FedStoreGraphId_value_null_or_empty";
    private static final Logger LOGGER = LoggerFactory.getLogger(Store.class);
    private static final List<Integer> ALL_IDS = new ArrayList<>();
    private FederatedGraphStorage graphStorage;
    private final int id;
    private Set<String> customPropertiesAuths;
    private Boolean isPublicAccessAllowed;
    private List<String> storeConfiguredGraphIds;
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    private Map<String, BiFunction> storeConfiguredMergeFunctions;
    private final Set<Class<? extends Operation>> externallySupportedOperations = new HashSet<>();

    @JsonCreator
    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "Random used once only and this class will not usually be created more than once")
    public FederatedStore(@JsonProperty("customPropertiesAuths") final Set<String> customPropertiesAuths,
                          @JsonProperty("isPublicAccessAllowed") final Boolean isPublicAccessAllowed,
                          @JsonProperty("storeConfiguredGraphIds") final List<String> storeConfiguredGraphIds,
                          @JsonProperty("storeConfiguredMergeFunctions") final Map<String, BiFunction> storeConfiguredMergeFunctions) {
        Integer i = null;
        while (isNull(i) || ALL_IDS.contains(i)) {
            i = new Random().nextInt();
        }
        ALL_IDS.add(id = i);

        this.customPropertiesAuths = customPropertiesAuths;
        this.isPublicAccessAllowed = (null == isPublicAccessAllowed) ? Boolean.valueOf(IS_PUBLIC_ACCESS_ALLOWED_DEFAULT) : isPublicAccessAllowed;
        this.storeConfiguredGraphIds = storeConfiguredGraphIds;
        this.storeConfiguredMergeFunctions = (null == storeConfiguredMergeFunctions) ? new HashMap<>() : new HashMap<>(storeConfiguredMergeFunctions);

        this.storeConfiguredMergeFunctions.putIfAbsent(GetTraits.class.getCanonicalName(), new CollectionIntersect<>());
        this.storeConfiguredMergeFunctions.putIfAbsent(GetAllElements.class.getCanonicalName(), new ApplyViewToElementsFunction());
        this.storeConfiguredMergeFunctions.putIfAbsent(GetElements.class.getCanonicalName(), new ApplyViewToElementsFunction());
        this.storeConfiguredMergeFunctions.putIfAbsent(GetSchema.class.getCanonicalName(), new MergeSchema());
    }

    public FederatedStore() {
        this(null, null, null, null);
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
        graphStorage = new FederatedGraphStorage(getCacheServiceFederatedStoreSuffix(properties, graphId));
        super.initialise(graphId, new Schema(), properties);
        final FederatedStoreProperties federatedProperties = getProperties();

        loadCustomPropertiesAuthFromProperties(federatedProperties);
        loadIsPublicAccessAllowedFromProperties(federatedProperties);
        loadStoreConfiguredMergeFunctionsFromProperties(federatedProperties);
        loadStoreConfiguredGraphIdsFromProperties(federatedProperties);
    }

    private void loadIsPublicAccessAllowedFromProperties(final FederatedStoreProperties federatedProperties) {
        isPublicAccessAllowed = Boolean.valueOf(federatedProperties.getIsPublicAccessAllowed(String.valueOf(isPublicAccessAllowed)));
    }

    private void loadStoreConfiguredGraphIdsFromProperties(final FederatedStoreProperties properties) throws StoreException {
        try {
            final List<String> configuredGraphIds = loadStoreConfiguredGraphIdsListFrom(properties.getStoreConfiguredGraphIds());
            if (nonNull(configuredGraphIds)) {
                //Overwrite with configured values
                this.storeConfiguredGraphIds = new ArrayList<>(configuredGraphIds);
            }
        } catch (final IOException e) {
            throw new StoreException("Error loading Merge Functions from StoreProperties.", e);
        }
    }

    private void loadStoreConfiguredMergeFunctionsFromProperties(final FederatedStoreProperties properties) throws StoreException {
        try {
            final Map<String, BiFunction> configuredMergeFunctions = loadStoreConfiguredMergeFunctionMapFrom(properties.getStoreConfiguredMergeFunctions());
            //Overwrite with configured values
            this.storeConfiguredMergeFunctions.putAll(configuredMergeFunctions);
        } catch (final IOException e) {
            throw new StoreException("Error loading Merge Functions from StoreProperties.", e);
        }
    }

    @Override
    public void setGraphLibrary(final GraphLibrary library) {
        super.setGraphLibrary(library);
        if (nonNull(graphStorage)) {
            graphStorage.setGraphLibrary(library);
        } else {
            throw new GafferRuntimeException("Error adding GraphLibrary, Initialise the FederatedStore first.");
        }
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
     * at initialisation, will default to usual access with owningUserId and
     * graphAuths
     * </p>
     *
     * @param owningUserId the adding userId
     * @param graphs       the graph to add
     * @param isPublic     if this class should have public access.
     * @param graphAuths   the access auths for the graph being added
     * @throws StorageException if unable to put graph into storage
     */
    public void addGraphs(final Set<String> graphAuths,
                          final String owningUserId,
                          final boolean isPublic,
                          final GraphSerialisable... graphs) throws StorageException {
        addGraphs(graphAuths, owningUserId, isPublic, null, null, graphs);
    }

    public void addGraphs(final Set<String> graphAuths,
                          final String owningUserId,
                          final boolean isPublic,
                          final AccessPredicate readAccessPredicate,
                          final AccessPredicate writeAccessPredicate,
                          final GraphSerialisable... graphs) throws StorageException {
        final FederatedAccess access = new FederatedAccess(graphAuths, owningUserId, isPublicAccessAllowed && isPublic, readAccessPredicate, writeAccessPredicate);
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
     * @param graphId     to be removed from scope
     * @param user        to match visibility against
     * @param removeCache to remove associated caches with this graph.
     * @return success of removal
     */
    public boolean remove(final String graphId, final User user, final boolean removeCache) {
        return remove(graphId, user, removeCache, false);
    }

    public boolean remove(final String graphId, final User user, final boolean removeCache, final boolean asAdmin) {
        return asAdmin
                ? graphStorage.remove(graphId, user, removeCache, this.getProperties().getAdminAuth())
                : graphStorage.remove(graphId, user, removeCache);
    }

    /**
     * @return a collection of all the {@link Operation}s supported by subgraphs.
     */
    public Set<Class<? extends Operation>> getExternallySupportedOperations() {
        return new HashSet<>(externallySupportedOperations);
    }

    public void removeExternallySupportedOperation(final Class<? extends Operation> operation) {
        externallySupportedOperations.remove(operation);
    }

    public void addExternallySupportedOperation(final Class<? extends Operation> operation) {
        externallySupportedOperations.add(operation);
    }

    /**
     * @param user the visibility to use for getting graphIds
     * @return All the graphId(s) within scope of this FederatedStore and within
     * visibility for the given user. These will be returned in random order.
     */
    public Collection<String> getAllGraphIds(final User user) {
        return getAllGraphIds(user, false);
    }

    public List<String> getAllGraphIds(final User user, final boolean userRequestingAdminUsage) {
        return userRequestingAdminUsage
                ? graphStorage.getAllIds(user, this.getProperties().getAdminAuth())
                : graphStorage.getAllIds(user);
    }

    /**
     * Get {@link Schema} for this FederatedStore (without a context)
     * <p>
     * This will return a merged schema of the optimised compact schemas
     * of the stores inside this FederatedStore. It uses an empty/default
     * {@link Context}, meaning it can only return results for graphs where
     * this is valid, an empty Schema is otherwise returned. To supply a
     * {@link Context}, use the {@link FederatedStore#getSchema} method,
     * or ideally use the {@link GetSchema} operation instead.
     *
     * @return {@link Schema}, empty if default {@link Context} not valid
     */
    @Override
    public Schema getSchema() {
        return getSchema(new Context(), true);
    }

    /**
     * Get {@link Schema} for this FederatedStore (without a context)
     * <p>
     * This will return a merged schema of the original schemas of the
     * stores inside this FederatedStore. This method uses an empty/default
     * {@link Context}, meaning it can only return results for graphs where
     * this is valid, an empty Schema is otherwise returned. To supply a
     * {@link Context}, use the {@link FederatedStore#getSchema} method,
     * or ideally use the {@link GetSchema} operation instead.
     *
     * @return {@link Schema}, empty if default {@link Context} not valid
     */
    @Override
    public Schema getOriginalSchema() {
        return getSchema(new Context(), false);
    }

    /**
     * Get {@link Schema} for this FederatedStore.
     * <p>
     * This will return a merged schema of the original schemas
     * or the optimised compact schemas of the stores inside
     * this FederatedStore.
     *
     * @param context          context with valid User
     * @param getCompactSchema if true, gets the optimised compact schemas
     * @return {@link Schema}
     */
    public Schema getSchema(final Context context, final boolean getCompactSchema) {
        try {
            return execute(new GetSchema.Builder().compact(getCompactSchema).build(), context);
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Unable to execute GetSchema Operation", e);
        }
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
     * if graphIdsCSV is null then all graph objects within FederatedStore
     * scope are returned.
     * </p>
     *
     * @param user      the users scope to get graphs for.
     * @param graphIds  the list of graphIds to get. null will return all graphs.
     * @param operation the requesting operation, graphs are returned only once per operation.
     * @return the graph collection.
     */
    public List<GraphSerialisable> getGraphs(final User user, final List<String> graphIds, final IFederationOperation operation) {
        List<GraphSerialisable> rtn = new ArrayList<>();
        if (nonNull(operation)) {
            boolean isFedStoreIdPreexisting = addFedStoreIdToOperation(operation);
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
            } else if (isNull(graphIds)) {
                LOGGER.debug("Getting default graphs because requested graphIds is null");
                rtn = getDefaultGraphs(user, operation);
            } else {
                if (graphIds.isEmpty()) {
                    LOGGER.debug("A get graphs request was made with empty graphIds");
                }
                String adminAuth = operation.isUserRequestingAdminUsage() ? this.getProperties().getAdminAuth() : null;
                rtn.addAll(new ArrayList<>(graphStorage.get(user, graphIds, adminAuth)));
            }
        } else {
            LOGGER.warn("getGraphs was requested with null Operation, this will return no graphs.");
        }
        return rtn;
    }

    private String getKeyForProcessedFedStoreId() {
        return FEDERATED_STORE_PROCESSED + id;
    }

    private boolean addFedStoreIdToOperation(final Operation operation) {
        final String keyForFedStoreId = getKeyForProcessedFedStoreId();
        boolean isFedStoreIdPreexisting = false;
        if (nonNull(operation) && !isNullOrEmpty(keyForFedStoreId)) {
            // KEEP THIS NUMBERED ORDER!
            // 1) Check operation for ID
            final boolean doesOperationHavePreexistingFedStoreId = operation.containsOption(keyForFedStoreId);
            final boolean doesFedStoreIDOptionHaveContent = !isNullOrEmpty(operation.getOption(keyForFedStoreId));

            // Log
            if (doesOperationHavePreexistingFedStoreId && !doesFedStoreIDOptionHaveContent) {
                //There is a slight difference between value null and key not found
                LOGGER.debug(String.format("The FederatedStoreId Key has a null or empty Value, this means the Key has been intentionally cleared for reprocessing by this FederatedStore. Key:%s", keyForFedStoreId));
            }

            // 2) Check and Add ID any payload for ID (recursion)
            final boolean doesPayloadHavePreexistingFedStoreId = (operation instanceof FederatedOperation)
                    && !doesFedStoreIDOptionHaveContent
                    && !doesOperationHavePreexistingFedStoreId
                    && addFedStoreIdToOperation(((FederatedOperation<?, ?>) operation).getUnClonedPayload());

            // 3) Add the ID
            operation.addOption(keyForFedStoreId, getValueForProcessedFedStoreId());

            // 4) return if the ID was found.
            isFedStoreIdPreexisting = doesFedStoreIDOptionHaveContent || doesPayloadHavePreexistingFedStoreId;
        }
        return isFedStoreIdPreexisting;
    }

    public Map<String, Object> getAllGraphsAndAuths(final User user, final List<String> graphIds, final boolean userRequestingAdminUsage) {
        return userRequestingAdminUsage
                ? graphStorage.getAllGraphsAndAccess(user, graphIds, this.getProperties().getAdminAuth())
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

        addOperationHandler(GetSchema.class, new FederatedOutputHandler<>(new Schema()));

        addOperationHandler(Filter.class, new FederatedDelegateToHandler(new FilterHandler()));
        addOperationHandler(Aggregate.class, new FederatedDelegateToHandler(new AggregateHandler()));
        addOperationHandler(Transform.class, new FederatedDelegateToHandler(new TransformHandler()));
        addOperationHandler(Validate.class, new FederatedDelegateToHandler(new ValidateHandler()));

        //override with Federated safe version.
        addOperationHandler(While.class, new FederatedWhileHandler());
        addOperationHandler(Join.class, new FederatedJoinHandler());

        //FederationOperations
        addOperationHandler(GetAllGraphIds.class, new FederatedGetAllGraphIDHandler());
        addOperationHandler(AddGraph.class, new FederatedAddGraphHandler());
        addOperationHandler(AddGraphWithHooks.class, new FederatedAddGraphWithHooksHandler());
        addOperationHandler(RemoveGraph.class, new FederatedRemoveGraphHandler());
        addOperationHandler(RemoveGraphAndDeleteAllData.class, new FederatedRemoveGraphAndDeleteAllDataHandler());

        addOperationHandler(GetAllGraphInfo.class, new FederatedGetAllGraphInfoHandler());
        addOperationHandler(ChangeGraphAccess.class, new FederatedChangeGraphAccessHandler());
        addOperationHandler(ChangeGraphId.class, new FederatedChangeGraphIdHandler());
        addOperationHandler(FederatedOperation.class, new FederatedOperationHandler());
    }

    @Override
    protected OperationChainValidator createOperationChainValidator() {
        return new FederatedOperationChainValidator(new FederatedViewValidator());
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return new FederatedOutputIterableHandler<>();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return new FederatedOutputIterableHandler<>();
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        return new FederatedNoOutputHandler<>();
    }

    @Override
    protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
        return new FederatedNoOutputHandler<>();
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new FederatedOutputIterableHandler<>();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new FederatedNoOutputHandler<>();
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        return new FederatedOutputHandler<>(/*default null value*/);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected void startCacheServiceLoader(final StoreProperties properties) {
        final String federatedStoreCacheClass = ((FederatedStoreProperties) properties).getFederatedStoreCacheServiceClass();
        final String defaultCacheClass = properties.getDefaultCacheServiceClass();
        if (federatedStoreCacheClass != null) {
            CacheServiceLoader.initialise(FEDERATED_STORE_CACHE_SERVICE_NAME, federatedStoreCacheClass, properties.getProperties());
        } else if (defaultCacheClass != null) {
            CacheServiceLoader.initialise(DEFAULT_SERVICE_NAME, defaultCacheClass, properties.getProperties());
        } else {
            LOGGER.warn("Federated Store Properties did not include a cache class, using '{}' as default", CACHE_SERVICE_CLASS_DEFAULT);
            properties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_DEFAULT);
            startCacheServiceLoader(properties);
        }
        super.startCacheServiceLoader(properties);
    }

    private void loadCustomPropertiesAuthFromProperties(final FederatedStoreProperties properties) {
        final String value = properties.getCustomPropsValue();
        final HashSet<String> customPropertiesAuthsFromProperties = (isNullOrEmpty(value)) ? null : new HashSet<>(getCleanStrings(value));

        if (isNull(customPropertiesAuths)) {
            customPropertiesAuths = customPropertiesAuthsFromProperties;
        } else {
            customPropertiesAuths.addAll(customPropertiesAuthsFromProperties);
        }
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

    public List<String> getStoreConfiguredGraphIds() {
        return storeConfiguredGraphIds;
    }

    private List<GraphSerialisable> getDefaultGraphs(final User user, final IFederationOperation operation) {
        if (isNull(storeConfiguredGraphIds)) {
            return graphStorage.get(user, null, (operation.isUserRequestingAdminUsage() ? getProperties().getAdminAuth() : null));
        } else {
            //This operation has already been processes once, by this store.
            String keyForProcessedFedStoreId = getKeyForProcessedFedStoreId();
            operation.addOption(keyForProcessedFedStoreId, ""); // value is empty, but key is still found.

            final List<String> graphIds = new ArrayList<>(storeConfiguredGraphIds);
            final List<String> federatedStoreSystemUser = getAllGraphIds(new User.Builder()
                            .userId(FEDERATED_STORE_SYSTEM_USER)
                            .opAuths(this.getProperties().getAdminAuth()).build(),
                    true);
            graphIds.retainAll(federatedStoreSystemUser);

            List<GraphSerialisable> graphs = getGraphs(user, graphIds, operation);
            //put it back
            operation.addOption(keyForProcessedFedStoreId, getValueForProcessedFedStoreId());
            return graphs;
        }
    }

    private String getValueForProcessedFedStoreId() {
        return isNullOrEmpty(getGraphId()) ? FED_STORE_GRAPH_ID_VALUE_NULL_OR_EMPTY : getGraphId();
    }

    public Map<String, BiFunction> getStoreConfiguredMergeFunctions() {
        return Collections.unmodifiableMap(storeConfiguredMergeFunctions);
    }

    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        try {
            if (operation instanceof Output) {
                if (Iterable.class.isAssignableFrom(((Output<?>) operation).getOutputClass())) {
                    return new FederatedOutputIterableHandler<>()
                            .doOperation((Output<Iterable<?>>) operation, context, this);
                } else {
                    return new FederatedOutputHandler<>()
                            .doOperation((Output<Object>) operation, context, this);
                }
            } else {
                return new FederatedNoOutputHandler()
                        .doOperation(operation, context, this);
            }
        } catch (final Exception e) {
            throw new UnsupportedOperationException(String.format("Operation class %s is not supported by the FederatedStore. Error occurred forwarding unhandled operation to sub-graphs due to: %s", operation.getClass().getName(), e.getMessage()), e);
        }
    }

}
