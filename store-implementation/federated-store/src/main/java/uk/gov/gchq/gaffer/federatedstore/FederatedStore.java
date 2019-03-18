/*
 * Copyright 2017-2019 Crown Copyright
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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllStoreIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveStore;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedAggregateHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedFilterHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedGetSchemaHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedTransformHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedValidateHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddStoreHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllGraphIDHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetTraitsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOperationChainHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedRemoveGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.operation.GetSchema;
import uk.gov.gchq.gaffer.graph.schema.Schema;
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
import uk.gov.gchq.gaffer.store.library.Library;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.util.Config;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.IS_PUBLIC_ACCESS_ALLOWED_DEFAULT;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getCleanStrings;

/**
 * <p>
 * A Store that encapsulates a collection of sub-graphs and executes operations
 * against them and returns results as though it was a single graph.
 * <p>
 * To create a FederatedStore you need to initialise the store with a
 * storeId and  (if storeId is not known by the {@link Library})
 * the {@link Config}.
 *
 * @see #initialise(Config)
 * @see Store
 */
public class FederatedStore extends Store {
    private FederatedStoreStorage storeStorage = new FederatedStoreStorage();
    private Set<String> customPropertiesAuths;
    private Boolean isPublicAccessAllowed = Boolean.valueOf(IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);

    /**
     * Initialise this FederatedStore with any sub-graphs defined within the
     * properties.
     *
     * @param config properties to initialise this FederatedStore with, can
     *               contain details on graphs to add to scope.
     * @throws StoreException if no cache has been set
     */
    @Override
    public void initialise(final Config config) throws StoreException {
        super.initialise(config);
        customPropertiesAuths = getCustomPropertiesAuths();
        isPublicAccessAllowed = Boolean.valueOf(getProperties().getIsPublicAccessAllowed());
    }

    public void setLibrary(final Library library) {
        super.getConfig().setLibrary(library);
        storeStorage.setlibrary(library);
    }

    /**
     * Get this Store's {@link uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties}.
     *
     * @return the instance of {@link uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties},
     * this may contain details such as database connection details.
     */
    public FederatedStoreProperties getProperties() {
        return (FederatedStoreProperties) super.getConfig().getProperties();
    }

    /**
     * <p>
     * Within FederatedStore an {@link Operation} is executed against a
     * collection of many graphs.
     * </p>
     * <p>
     * Problem: When an Operation contains View information about an Element
     * which is not known by the Graph; It will fail validation when executed.
     * </p>
     * <p>
     * Solution: For each operation, remove all elements from the View that is
     * unknown to the graph.
     * </p>
     *
     * @param operation current operation
     * @param store     current store
     * @param <OP>      Operation type
     * @return cloned operation with modified View for the given graph.
     * @deprecated see {@link FederatedStoreUtil#updateOperationForStore(Operation, Store)}
     */
    @Deprecated
    public static <OP extends Operation> OP updateOperationForStore(final OP operation, final Store store) {
        return FederatedStoreUtil.updateOperationForStore(operation, store);
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
     * @param stores       the stores to add
     * @param isPublic     if this class should have public access.
     * @param storeAuths   the access auths for the graph being added
     * @throws StorageException if unable to put graph into storage
     */
    public void addStores(final Set<String> storeAuths,
                          final String addingUserId, final boolean isPublic,
                          final Store... stores) throws StorageException {
        addStores(storeAuths, addingUserId, isPublic,
                FederatedStoreStorage.DEFAULT_DISABLED_BY_DEFAULT, stores);
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
     * @param stores            the stores to add
     * @param isPublic          if this class should have public access.
     * @param disabledByDefault true if the graph should be disabled by default - requiring the graphId to be provided in queries
     * @param storeAuths        the access auths for the stores being added
     * @throws StorageException if unable to put graph into storage
     */
    public void addStores(final Set<String> storeAuths,
                         final String addingUserId, final boolean isPublic,
                          final boolean disabledByDefault,
                          final Store... stores) throws StorageException {
        final FederatedAccess access = new FederatedAccess(storeAuths, addingUserId,
                isPublicAccessAllowed && isPublic, disabledByDefault);
        addStores(access, stores);
    }

    public void addStores(final FederatedAccess access,
                     final Store... stores) throws StorageException {
        for (final Store store : stores) {
            _add(store, access);
        }
    }

    @Deprecated
    public void addStore(final Set<String> storeAuths,
                         final String addingUserId, final Store... stores) throws StorageException {
        addStores(storeAuths, addingUserId, false, stores);
    }

    /**
     * <p>
     * Removes graphs from the scope of FederatedStore.
     * </p>
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should remove
     * graphs via the {@link RemoveStore} operation.
     * </p>
     *
     * @param storeId to be removed from scope
     * @param user    to match visibility against
     */
    public void remove(final String storeId, final User user) {
        storeStorage.remove(storeId, user);
    }

    /**
     * @param user the visibility to use for getting graphIds
     * @return All the graphId(s) within scope of this FederatedStore and within
     * visibility for the given user.
     */
    public Collection<String> getAllStoreIds(final User user) {
        return storeStorage.getAllIds(user);
    }

    public Schema getSchema() {
        return getSchema((Map<String, String>) null, (User) null);
    }

    public Schema getSchema(final GetSchema operation, final Context context) {
        if (null == operation) {
            return getSchema((Map<String, String>) null, context);
        }

        return storeStorage.getSchema(operation, context);
    }

    public Schema getSchema(final Operation operation, final Context context) {
        if (null == operation) {
            return getSchema((Map<String, String>) null, context);
        }

        return getSchema(operation.getOptions(), context);
    }

    public Schema getSchema(final Map<String, String> config, final Context context) {
        return storeStorage.getSchema(config, context);
    }

    public Schema getSchema(final Operation operation, final User user) {
        if (null == operation) {
            return getSchema((Map<String, String>) null, user);
        }

        return getSchema(operation.getOptions(), user);
    }

    public Schema getSchema(final Map<String, String> config, final User user) {
        return storeStorage.getSchema(config, user);
    }

    /**
     * @return {@link Store#getTraits()}
     */
    @Override
    public Set<StoreTrait> getTraits() {
        return StoreTrait.ALL_TRAITS;
    }

    public Set<StoreTrait> getTraits(final GetTraits getTraits, final Context context) {
        return storeStorage.getTraits(getTraits, context);
    }

    /**
     * <p>
     * Gets a collection of graph objects within FederatedStore scope from the
     * given csv of graphIds, with visibility of the given user.
     * </p>
     * <p>
     * if graphIdsCsv is null then all graph objects within FederatedStore
     * scope are returned.
     * </p>
     *
     * @param user        the users scope to get graphs for.
     * @param storeIdsCsv the csv of graphIds to get, null returns all graphs.
     * @return the graph collection.
     */
    public Collection<Store> getStores(final User user,
                                     final String storeIdsCsv) {
        return storeStorage.get(user, getCleanStrings(storeIdsCsv));
    }

    /**
     * The FederatedStore at time of initialisation, can set the auths required
     * to allow users to use custom {@link StoreProperties} outside the
     * scope of the {@link Library}.
     *
     * @param user the user needing validation for custom property usage.
     * @return boolean permission
     */
    public boolean isLimitedToLibraryProperties(final User user) {
        return (null != this.customPropertiesAuths) && Collections.disjoint(user.getOpAuths(), this.customPropertiesAuths);
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // Override the Operations that don't have an output
        getSupportedOperations()
                .stream()
                .filter(op -> !Output.class.isAssignableFrom(op)
                        && !AddElements.class.equals(op)
                        && !AddNamedOperation.class.equals(op)
                        && !AddNamedView.class.equals(op))
                .forEach(op -> getConfig().addOperationHandler(op, new FederatedOperationHandler()));

        getConfig().addOperationHandler(GetSchema.class,
                new FederatedGetSchemaHandler());

        getConfig().addOperationHandler(Filter.class, new FederatedFilterHandler());
        getConfig().addOperationHandler(Aggregate.class, new FederatedAggregateHandler());
        getConfig().addOperationHandler(Transform.class, new FederatedTransformHandler());

        getConfig().addOperationHandler(Validate.class, new FederatedValidateHandler());

        getConfig().addOperationHandler(GetAllStoreIds.class, new FederatedGetAllGraphIDHandler());
        getConfig().addOperationHandler(AddGraph.class, new FederatedAddStoreHandler());
       // getConfig().addOperationHandler(AddGraphWithHooks.class,
               // new FederatedAddStoreWithHooksHandler());
        getConfig().addOperationHandler(RemoveStore.class, new FederatedRemoveGraphHandler());

        getConfig().addOperationHandler(FederatedOperationChain.class, new FederatedOperationChainHandler());
        getConfig().addOperationHandler(GetTraits.class, new FederatedGetTraitsHandler());
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new FederatedGetElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
        return new FederatedGetAllElementsHandler();
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new FederatedGetAdjacentIdsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return (OperationHandler) new FederatedOperationHandler();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation,
                                          final Context context) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void startCacheServiceLoader(final StoreProperties properties) {
        super.startCacheServiceLoader(properties);
        try {
            storeStorage.startCacheServiceLoader();
        } catch (final StorageException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Set<String> getCustomPropertiesAuths() {
        final String value = getProperties().getCustomPropsValue();
        return (Strings.isNullOrEmpty(value)) ? null : Sets.newHashSet(getCleanStrings(value));
    }

    private void _add(final Store newStore, final FederatedAccess access) throws StorageException {
        storeStorage.put(newStore, access);
    }
}
