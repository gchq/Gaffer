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

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.operation.GetSchema;
import uk.gov.gchq.gaffer.graph.schema.Schema;
import uk.gov.gchq.gaffer.graph.schema.Schema.Builder;
import uk.gov.gchq.gaffer.graph.util.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.Library;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_STORE_IDS;

public class FederatedStoreStorage {
    public static final boolean DEFAULT_DISABLED_BY_DEFAULT = false;
    public static final String ERROR_ADDING_GRAPH_TO_CACHE = "Error adding graph, StoreId is known within the cache, but %s is different. StoreId: %s";
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE = "User is attempting to overwrite a graph within FederatedStore. StoreId: %s";
    public static final String ACCESS_IS_NULL = "Can not put graph into storage without a FederatedAccess key.";
    public static final String GRAPH_IDS_NOT_VISIBLE = "The following graphIds are not visible or do not exist: %s";
    public static final String UNABLE_TO_MERGE_THE_SCHEMAS_FOR_ALL_OF_YOUR_FEDERATED_GRAPHS = "Unable to merge the schemas for all of your federated graphs: %s. You can limit which graphs to query for using the operation option: %s";
    private Map<FederatedAccess, Set<Store>> storage = new HashMap<>();
    private FederatedStoreCache federatedStoreCache = new FederatedStoreCache();
    private Boolean isCacheEnabled = false;
    private Library library;

    protected void startCacheServiceLoader() throws StorageException {
        if (CacheServiceLoader.isEnabled()) {
            isCacheEnabled = true;
            makeAllStoresFromCache();
        }
    }

    /**
     * places a collections of graphs into storage, protected by the given
     * access.
     *
     * @param stores the stores to add to the storage.
     * @param access access required to for the graphs, can't be null
     * @throws StorageException if unable to put arguments into storage
     * @see #put(Store, FederatedAccess)
     */
    public void put(final Collection<Store> stores,
                    final FederatedAccess access) throws StorageException {
        for (final Store store : stores) {
            put(store, access);
        }
    }

    /**
     * places a graph into storage, protected by the given access.
     * <p> StoreId can't already exist, otherwise {@link
     * OverwritingException} is thrown.
     * <p> Access can't be null otherwise {@link IllegalArgumentException} is
     * thrown
     *
     * @param store  the graph to add to the storage.
     * @param access access required to for the graph.
     * @throws StorageException if unable to put arguments into storage
     */
    public void put(final Store store, final FederatedAccess access) throws StorageException {
        if (store != null) {
            String storeId = store.getId();
            try {
                if (null == access) {
                    throw new IllegalArgumentException(ACCESS_IS_NULL);
                }

                if (null != library) {
                    library.checkExisting(storeId, store.getConfig());
                }

                validateExisting(store);
                if (isCacheEnabled()) {
                    addToCache(store, access);
                }

                Set<Store> existingStores = storage.get(access);
                if (null == existingStores) {
                    existingStores = Sets.newHashSet(store);
                    storage.put(access, existingStores);
                } else {
                    existingStores.add(store);
                }
            } catch (final Exception e) {
                throw new StorageException("Error adding graph " + storeId + " to " +
                        "storage due to: " + e.getMessage(), e);
            }
        } else {
            throw new StorageException("Store cannot be null");
        }
    }


    /**
     * Returns all the graphIds that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphIds.
     */
    public Collection<String> getAllIds(final User user) {
        final Set<String> rtn = getAllStream(user)
                .map(Store::getId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Collections.unmodifiableSet(rtn);
    }

    /**
     * Returns all graph object that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphs
     */
    public Collection<Store> getAll(final User user) {
        final Set<Store> rtn = getAllStream(user)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableCollection(rtn);
    }

    /**
     * Removes a graph from storage and returns the success. The given user
     * must
     * have visibility of the graph to be able to remove it.
     *
     * @param storeId the graphId to remove.
     * @param user    to match visibility against.
     * @return if a graph was removed.
     * @see #isValidToView(User, FederatedAccess)
     */
    public boolean remove(final String storeId, final User user) {
        boolean isRemoved = false;
        for (final Entry<FederatedAccess, Set<Store>> entry :
                storage.entrySet()) {
            if (isValidToView(user, entry.getKey())) {
                final Set<Store> stores = entry.getValue();
                if (null != stores) {
                    HashSet<Store> remove = Sets.newHashSet();
                    for (final Store store : stores) {
                        if (store.getId().equals(storeId)) {
                            remove.add(store);
                            deleteFromCache(storeId);
                            isRemoved = true;
                        }
                    }
                    stores.removeAll(remove);
                }
            }
        }
        return isRemoved;
    }

    private void deleteFromCache(final String storeId) {
        if (isCacheEnabled()) {
            federatedStoreCache.deleteFromCache(storeId);
        }
    }

    /**
     * returns all graphs objects matching the given graphIds, that is visible
     * to the user.
     *
     * @param user     to match visibility against.
     * @param storeIds the graphIds to get graphs for.
     * @return visible graphs from the given graphIds.
     */
    public Collection<Store> get(final User user, final List<String> storeIds) {
        if (null == user) {
            return Collections.emptyList();
        }

        validateAllGivenStoreIdsAreVisibleForUser(user, storeIds);
        Stream<Store> stores = getStream(user, storeIds);
        if (null != storeIds) {
            stores =
                    stores.sorted((s1, s2) -> storeIds.indexOf(s1.getId()) - storeIds.indexOf(s2.getId()));
        }
        final Set<Store> rtn =
                stores.collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableCollection(rtn);
    }

    public Schema getSchema(final GetSchema operation, final Context context) {
        if (null == context || null == context.getUser()) {
            // no user then return an empty schema
            return new Schema();
        }

        if (null == operation) {
            return getSchema((Map<String, String>) null, context);
        }

        final List<String> storeIds =
                FederatedStoreUtil.getstoreIds(operation.getOptions());
        final Stream<Store> stores = getStream(context.getUser(), storeIds);
        final Builder schemaBuilder = new Builder();
        try {
            if (operation.isCompact()) {
                final GetSchema getSchema = new GetSchema.Builder()
                        .compact(true)
                        .build();
                stores.forEach(g -> {
                    try {
                        schemaBuilder.merge(g.execute(getSchema, context));
                    } catch (final OperationException e) {
                        throw new RuntimeException("Unable to fetch schema " +
                                "from graph " + g.getId(), e);
                    }
                });
            } else {
                stores.forEach(s -> schemaBuilder.merge(((GraphConfig) s.getConfig()).getSchema()));
            }
        } catch (final SchemaException e) {
            final List<String> resultStoreIds =
                    getStream(context.getUser(), storeIds).map(Store::getId).collect(Collectors.toList());
            throw new SchemaException("Unable to merge the schemas for all of" +
                    " your federated graphs: " + resultStoreIds + ". You can limit which" +
                    " graphs to query for using the operation option: " + KEY_OPERATION_OPTIONS_STORE_IDS, e);
        }
        return schemaBuilder.build();
    }

    /**
     * @param config  configuration containing optional graphIds
     * @param context the user context to match visibility against.
     * @return merged schema of the visible graphs.
     */
    public Schema getSchema(final Map<String, String> config, final Context context) {
        if (null == context) {
            // no context then return an empty schema
            return new Schema();
        }

        return getSchema(config, context.getUser());
    }

    public Schema getSchema(final Map<String, String> config, final User user) {
        if (null == user) {
            // no user then return an empty schema
            return new Schema();
        }

        final List<String> storeIds = FederatedStoreUtil.getstoreIds(config);
        final Stream<Store> stores = getStream(user, storeIds);
        final Builder schemaBuilder = new Builder();
        try {
            stores.forEach(s -> schemaBuilder.merge(((GraphConfig) s.getConfig()).getSchema()));
        } catch (final SchemaException e) {
            final List<String> resultStoreIds =
                    getStream(user, storeIds).map(Store::getId).collect(Collectors.toList());
            throw new SchemaException(String.format(UNABLE_TO_MERGE_THE_SCHEMAS_FOR_ALL_OF_YOUR_FEDERATED_GRAPHS, resultStoreIds, KEY_OPERATION_OPTIONS_STORE_IDS), e);
        }
        return schemaBuilder.build();
    }

    /**
     * returns a set of {@link StoreTrait} that are common for all visible graphs.
     * traits1 = [a,b,c]
     * traits2 = [b,c]
     * traits3 = [a,b]
     * return [b]
     *
     * @param op      the GetTraits operation
     * @param context the user context
     * @return the set of {@link StoreTrait} that are common for all visible graphs
     */
    public Set<StoreTrait> getTraits(final GetTraits op, final Context context) {
        final Set<StoreTrait> traits = Sets.newHashSet(StoreTrait.values());
        if (null != op && op.isCurrentTraits()) {
            final List<String> graphIds =
                    FederatedStoreUtil.getstoreIds(op.getOptions());
            final Stream<Store> stores = getStream(context.getUser(), graphIds);
            final GetTraits getTraits = op.shallowClone();
            stores.forEach(s -> {
                try {
                    traits.retainAll(s.execute(getTraits, context));
                } catch (final OperationException e) {
                    throw new RuntimeException("Unable to fetch traits from " +
                            "graph " + s.getId(), e);
                }
            });
        }

        return traits;
    }

    /**
     * returns a set of {@link StoreTrait} that are common for all visible graphs.
     * traits1 = [a,b,c]
     * traits2 = [b,c]
     * traits3 = [a,b]
     * return [b]
     *
     * @param config containing optional graphIds csv.
     * @param user   to match visibility against.
     * @return the set of {@link StoreTrait} that are common for all visible graphs
     */
    public Set<StoreTrait> getTraits(final Map<String, String> config, final User user) {
        final List<String> storeIds = FederatedStoreUtil.getstoreIds(config);
        Collection<Store> stores = get(user, storeIds);

        final Set<StoreTrait> traits = stores.isEmpty() ? Sets.newHashSet() :
                Sets.newHashSet(StoreTrait.values());
        for (final Store store : stores) {
            traits.retainAll(store.getTraits());
        }
        return traits;
    }

    private void validateAllGivenStoreIdsAreVisibleForUser(final User user,
                                                           final Collection<String> storeIds) {
        if (null != storeIds) {
            final Collection<String> visibleIds = getAllIds(user);
            if (!visibleIds.containsAll(storeIds)) {
                final Set<String> notVisibleIds = Sets.newHashSet(storeIds);
                notVisibleIds.removeAll(visibleIds);
                throw new IllegalArgumentException(String.format(GRAPH_IDS_NOT_VISIBLE, notVisibleIds));
            }
        }
    }

    private void validateExisting(final Store store) throws StorageException {
        final String graphId = store.getId();
        for (final Set<Store> stores : storage.values()) {
            for (final Store s : stores) {
                if (s.getId().equals(graphId)) {
                    throw new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, graphId)));
                }
            }
        }
    }

    /**
     * @param user   to match visibility against, if null will default to
     *               false/denied
     *               access
     * @param access access the user must match.
     * @return the boolean access
     */
    private boolean isValidToView(final User user, final FederatedAccess access) {
        return null != access && access.isValidToExecute(user);
    }

    /**
     * @param user     to match visibility against.
     * @param storeIds filter on graphIds
     * @return a stream of graphs for the given graphIds and the user has visibility for.
     * If graphIds is null then only enabled by default graphs are returned that the user can see.
     */
    private Stream<Store> getStream(final User user,
                                    final Collection<String> storeIds) {
        if (null == storeIds) {
            return storage.entrySet()
                    .stream()
                    .filter(entry -> isValidToView(user, entry.getKey()))
                    .filter(entry -> !entry.getKey().isDisabledByDefault())
                    .flatMap(entry -> entry.getValue().stream());
        }

        return storage.entrySet()
                .stream()
                .filter(entry -> isValidToView(user, entry.getKey()))
                .flatMap(entry -> entry.getValue().stream())
                .filter(store -> storeIds.contains(store.getId()));
    }

    /**
     * @param user to match visibility against
     * @return graphs that are enabled by default and the user has visibility of.
     */
    private Stream<Store> getStream(final User user) {
        return getStream(user, null);
    }

    /**
     * @param user to match visibility against.
     * @return a stream of graphs the user has visibility for.
     */
    private Stream<Store> getAllStream(final User user) {
        return storage.entrySet()
                .stream()
                .filter(entry -> isValidToView(user, entry.getKey()))
                .flatMap(entry -> entry.getValue().stream());
    }

    private void addToCache(final Store newStore,
                            final FederatedAccess access) {
        final String storeId = newStore.getId();
        if (federatedStoreCache.contains(storeId)) {
            validateSameAsFromCache(newStore, storeId);
        } else {
            try {
                federatedStoreCache.addStoreToCache(newStore, access, false);
            } catch (final OverwritingException e) {
                throw new OverwritingException((String.format("User is " +
                        "attempting to overwrite a graph within the " +
                        "cacheService. StoreId: %s", storeId)));
            } catch (final CacheOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void validateSameAsFromCache(final Store newStore,
                                         final String storeId) {
        final Store fromCache =
                federatedStoreCache.getStoreFromCache(storeId);
        if (!newStore.getConfig().equals(fromCache.getConfig())) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, StoreConfigEnum.PROPERTIES.toString(), storeId));
        }
    }

    public void setlibrary(final Library library) {
        this.library = library;
    }

    /**
     * Enum for the Store Properties or Schema
     */
    public enum StoreConfigEnum {
        SCHEMA("schema"), PROPERTIES("properties");

        private final String value;

        StoreConfigEnum(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

    }

    private Boolean isCacheEnabled() {
        boolean rtn = false;
        if (isCacheEnabled) {
            if (federatedStoreCache.getCache() == null) {
                throw new RuntimeException("No cache has been set, please initialise the FederatedStore instance");
            }
            rtn = true;
        }
        return rtn;
    }

    private void makeStoreFromCache(final String storeId) throws StorageException {
        final Store store =
                federatedStoreCache.getStoreFromCache(storeId);
        final FederatedAccess accessFromCache =
                federatedStoreCache.getAccessFromCache(storeId);
        put(store, accessFromCache);
    }

    private void makeAllStoresFromCache() throws StorageException {
        final Set<String> allStoreIds = federatedStoreCache.getAllStoreIds();
        for (final String graphId : allStoreIds) {
            makeStoreFromCache(graphId);
        }
    }
}
