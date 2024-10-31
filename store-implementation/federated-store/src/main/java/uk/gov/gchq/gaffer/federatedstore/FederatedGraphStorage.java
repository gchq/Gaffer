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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.renameTable;

/**
 * @deprecated Will be removed in 2.4.0.
 */
@Deprecated
public class FederatedGraphStorage {
    public static final String ERROR_ADDING_GRAPH_TO_CACHE = "Error adding graph, GraphId is known within the cache, but %s is different. GraphId: %s";
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    public static final String ACCESS_IS_NULL = "Can not put graph into storage without a FederatedAccess key.";
    public static final String GRAPH_IDS_NOT_VISIBLE = "The following graphIds are not visible or do not exist: %s";
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedGraphStorage.class);
    private final FederatedStoreCache federatedStoreCache;
    private GraphLibrary graphLibrary;

    public FederatedGraphStorage(final String suffixFederatedStoreCacheName) {
        federatedStoreCache = new FederatedStoreCache(suffixFederatedStoreCacheName);
    }

    /**
     * places a collections of graphs into storage, protected by the given
     * access.
     *
     * @param graphs the graphs to add to the storage.
     * @param access access required to for the graphs, can't be null
     * @throws StorageException if unable to put arguments into storage
     * @see #put(GraphSerialisable, FederatedAccess)
     */
    public void put(final Collection<GraphSerialisable> graphs, final FederatedAccess access) throws StorageException {
        for (final GraphSerialisable graph : graphs) {
            put(graph, access);
        }
    }

    /**
     * places a graph into storage, protected by the given access.
     * <p> GraphId can't already exist, otherwise {@link OverwritingException} is thrown.
     * <p> Access can't be null otherwise {@link IllegalArgumentException} is
     * thrown
     *
     * @param graph  the graph to add to the storage.
     * @param access access required to for the graph.
     * @throws StorageException if unable to put arguments into storage
     */
    public void put(final GraphSerialisable graph, final FederatedAccess access) throws StorageException {
        if (graph != null) {
            String graphId = graph.getGraphId();
            try {
                if (null == access) {
                    throw new StorageException(new IllegalArgumentException(ACCESS_IS_NULL));
                }

                if (null != graphLibrary) {
                    graphLibrary.checkExisting(graphId, graph.getSchema(graphLibrary), graph.getStoreProperties(graphLibrary));
                }

                validateExisting(graphId);

                addToCache(graph, access);

            } catch (final Exception e) {
                throw new StorageException(String.format("Error adding graph %s%s", graphId,
                        nonNull(e.getMessage())
                                ? (" to storage due to: " + e.getMessage())
                                : "."), e);
            }
        } else {
            throw new StorageException("Graph cannot be null");
        }
    }


    /**
     * Returns all the graphIds that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphIds.
     */
    public List<String> getAllIds(final User user) {
        return getIdsFrom(getUserGraphStream(federatedAccess -> federatedAccess.hasReadAccess(user)));
    }

    public List<String> getAllIds(final User user, final String adminAuth) {
        return getIdsFrom(getUserGraphStream(federatedAccess -> federatedAccess.hasReadAccess(user, adminAuth)));
    }

    private List<String> getIdsFrom(final Stream<GraphSerialisable> allStream) {
        final List<String> rtn = allStream
                .map(GraphSerialisable::getGraphId)
                .distinct()
                .collect(Collectors.toList());

        return Collections.unmodifiableList(rtn);
    }

    /**
     * Returns all graph object that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphs
     */
    public Collection<GraphSerialisable> getAll(final User user) {
        final Collection<GraphSerialisable> rtn = getUserGraphStream(federatedAccess -> federatedAccess.hasReadAccess(user))
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
        return Collections.unmodifiableCollection(rtn);
    }

    /**
     * Removes a graph from storage and returns the success. The given user
     * must
     * have visibility of the graph to be able to remove it.
     *
     * @param graphId     the graphId to remove.
     * @param user        to match visibility against.
     * @param removeCache to remove associated caches with this graph.
     * @return if a graph was removed.
     * @see #isValidToView(User, FederatedAccess)
     */
    public boolean remove(final String graphId, final User user, final boolean removeCache) {
        return remove(graphId, federatedAccess -> federatedAccess.hasWriteAccess(user), removeCache);
    }

    protected boolean remove(final String graphId, final User user, final boolean removeCache, final String adminAuth) {
        return remove(graphId, federatedAccess -> federatedAccess.hasWriteAccess(user, adminAuth), removeCache);
    }

    private boolean remove(final String graphId, final Predicate<FederatedAccess> accessPredicate, final boolean removeCache) {
        FederatedAccess accessFromCache = federatedStoreCache.getAccessFromCache(graphId);
        boolean rtn;
        if (nonNull(accessFromCache) && accessPredicate.test(accessFromCache)) {

            if (removeCache) {
                removeGraphCaches(graphId);
            }

            federatedStoreCache.deleteFromCache(graphId);
            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
    }

    private void removeGraphCaches(final String graphId) {
        try {
            final GraphSerialisable graphFromCache = federatedStoreCache.getGraphFromCache(graphId);
            for (final Cache<?, ?> cacheInstance : graphFromCache.getGraph().getCaches()) {
                final ICache<?, ?> cache = cacheInstance.getCache();
                if (nonNull(cache)) {
                    cache.clear();
                } else {
                    LOGGER.debug(String.format("No cache found graphId:%s with cacheName:%s", graphId, cacheInstance.getCacheName()));
                }
            }
        } catch (final CacheOperationException e) {
            throw new GafferRuntimeException(String.format("Error clearing Cache while removing graphId: %s", graphId), e);
        }
    }

    /**
     * returns all graphs objects matching the given graphIds, that is visible
     * to the user.
     *
     * @param user     to match visibility against.
     * @param graphIds the graphIds to get graphs for.
     * @return visible graphs from the given graphIds.
     */
    public Collection<GraphSerialisable> get(final User user, final List<String> graphIds) {
        return get(user, graphIds, null);
    }

    /**
     * returns all graphs objects matching the given graphIds, that is visible
     * to the user.
     *
     * @param user      to match visibility against.
     * @param graphIds  the graphIds to get graphs for. List is used because it preserves order.
     * @param adminAuth adminAuths role
     * @return visible graphs from the given graphIds.
     */
    public List<GraphSerialisable> get(final User user, final List<String> graphIds, final String adminAuth) {
        if (null == user) {
            return Collections.emptyList();
        }

        validateAllGivenGraphIdsAreVisibleForUser(user, graphIds, adminAuth);
        Stream<GraphSerialisable> graphs = getStream(user, graphIds);
        if (null != graphIds) {
            //This maintains order with the requested Ids.
            graphs = graphs.sorted(Comparator.comparingInt(g -> graphIds.indexOf(g.getGraphId())));
        }
        final List<GraphSerialisable> rtn = graphs.distinct().collect(Collectors.toList());
        return Collections.unmodifiableList(rtn);
    }

    private void validateAllGivenGraphIdsAreVisibleForUser(final User user, final Collection<String> graphIds, final String adminAuth) {
        if (null != graphIds) {
            final Collection<String> visibleIds = getAllIds(user, adminAuth);
            if (!visibleIds.containsAll(graphIds)) {
                final List<String> notVisibleIds = new ArrayList<>(graphIds);
                notVisibleIds.removeAll(visibleIds);
                throw new IllegalArgumentException(String.format(GRAPH_IDS_NOT_VISIBLE, notVisibleIds));
            }
        }
    }

    private void validateExisting(final String graphId) throws StorageException {
        boolean exists = federatedStoreCache.contains(graphId);
        if (exists) {
            throw new StorageException(new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, graphId))));
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
        return null != access && access.hasReadAccess(user);
    }

    /**
     * @param user     to match visibility against.
     * @param graphIds filter on graphIds
     * @return a stream of graphs for the given graphIds and the user has visibility for.
     * If graphIds is null then only enabled by default graphs are returned that the user can see.
     */
    private Stream<GraphSerialisable> getStream(final User user, final Collection<String> graphIds) {
        Stream<GraphSerialisable> rtn;
        if (isNull(graphIds)) {
            rtn = StreamSupport.stream(federatedStoreCache.getAllGraphIds().spliterator(), false)
                    .map(federatedStoreCache::getFromCache)
                    .filter(pair -> isValidToView(user, pair.getSecond()))
                    .map(Pair::getFirst);
        } else {
            rtn = StreamSupport.stream(federatedStoreCache.getAllGraphIds().spliterator(), false)
                    .map(federatedStoreCache::getFromCache)
                    .filter(pair -> isValidToView(user, pair.getSecond()))
                    .filter(pair -> graphIds.contains(pair.getFirst().getGraphId()))
                    .map(Pair::getFirst);
        }
        return rtn;
    }

    /**
     * @param readAccessPredicate to filter graphs.
     * @return a stream of graphs the user has visibility for.
     */
    private Stream<GraphSerialisable> getUserGraphStream(final Predicate<FederatedAccess> readAccessPredicate) {
        return StreamSupport.stream(federatedStoreCache.getAllGraphIds().spliterator(), false)
            .map(federatedStoreCache::getFromCache)
            .filter(pair -> readAccessPredicate.test(pair.getSecond()))
            .map(Pair::getFirst);
    }

    @SuppressWarnings("PMD.PreserveStackTrace") //Not Required
    private void addToCache(final GraphSerialisable newGraph, final FederatedAccess access) {
        if (federatedStoreCache.contains(newGraph.getGraphId())) {
            validateSameAsFromCache(newGraph);
        } else {
            try {
                federatedStoreCache.addGraphToCache(newGraph, access, false);
            } catch (final OverwritingException e) {
                throw new OverwritingException((String.format("User is attempting to overwrite a graph within the cacheService. GraphId: %s", newGraph.getGraphId())));
            } catch (final CacheOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void validateSameAsFromCache(final GraphSerialisable newGraph) {
        String graphId = newGraph.getGraphId();

        GraphSerialisable fromCache = federatedStoreCache.getGraphSerialisableFromCache(graphId);

        if (!newGraph.getStoreProperties(graphLibrary).getProperties().equals(fromCache.getStoreProperties(graphLibrary).getProperties())) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, "properties", graphId));
        }
        if (!JsonUtil.equals(newGraph.getSchema(graphLibrary).toJson(false), fromCache.getSchema(graphLibrary).toJson(false))) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, "schema", graphId));
        }
        if (!newGraph.getGraphId().equals(fromCache.getGraphId())) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, "GraphId", graphId));
        }
    }

    public void setGraphLibrary(final GraphLibrary graphLibrary) {
        this.graphLibrary = graphLibrary;
    }

    protected Map<String, Object> getAllGraphsAndAccess(final User user, final List<String> graphIds) {
        return getAllGraphsAndAccess(graphIds, access -> access != null && access.hasReadAccess(user));
    }

    protected Map<String, Object> getAllGraphsAndAccess(final User user, final List<String> graphIds, final String adminAuth) {
        return getAllGraphsAndAccess(graphIds, access -> access != null && access.hasReadAccess(user, adminAuth));
    }

    private Map<String, Object> getAllGraphsAndAccess(final List<String> graphIds, final Predicate<FederatedAccess> accessPredicate) {
        return StreamSupport.stream(federatedStoreCache.getAllGraphIds().spliterator(), false)
            .map(federatedStoreCache::getFromCache)
            // filter on FederatedAccess
            .filter(pair -> accessPredicate.test(pair.getSecond()))
            // filter on if graph required?
            .filter(pair -> {
                final boolean isGraphIdRequested = nonNull(graphIds) && graphIds.contains(pair.getFirst().getGraphId());
                final boolean isAllGraphIdsRequired = isNull(graphIds) || graphIds.isEmpty();
                return isGraphIdRequested || isAllGraphIdsRequired;
            })
            .collect(Collectors.toMap(pair -> pair.getFirst().getGraphId(), Pair::getSecond));
    }

    public boolean changeGraphAccess(final String graphId, final FederatedAccess newFederatedAccess, final User requestingUser) throws StorageException {
        return changeGraphAccess(graphId, newFederatedAccess, access -> access.hasWriteAccess(requestingUser));
    }

    public boolean changeGraphAccess(final String graphId, final FederatedAccess newFederatedAccess, final User requestingUser, final String adminAuth) throws StorageException {
        return changeGraphAccess(graphId, newFederatedAccess, access -> access.hasWriteAccess(requestingUser, adminAuth));
    }

    private boolean changeGraphAccess(final String graphId, final FederatedAccess newFederatedAccess, final Predicate<FederatedAccess> accessPredicate) throws StorageException {
        boolean rtn;
        final GraphSerialisable graphToUpdate = getGraphToUpdate(graphId, accessPredicate);

        if (nonNull(graphToUpdate)) {
            //remove graph to be moved
            remove(graphId, federatedAccess -> true, false);

            updateCacheWithNewAccess(graphId, newFederatedAccess, graphToUpdate);

            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
    }

    private void updateCacheWithNewAccess(final String graphId, final FederatedAccess newFederatedAccess, final GraphSerialisable graphToUpdate) throws StorageException {
        try {
            this.put(new GraphSerialisable.Builder(graphToUpdate).build(), newFederatedAccess);
        } catch (final Exception e) {
            String message = String.format("Error occurred updating graphAccess. GraphStorage=updated, Cache=outdated. graphId:%s. Recovery is possible from a restart if a persistent cache is being used, otherwise contact admin", graphId);
            LOGGER.error("{} graphStorage access:{} cache access:{}", message, newFederatedAccess, federatedStoreCache.getAccessFromCache(graphId));
            throw new StorageException(message, e);
        }
    }

    public boolean changeGraphId(final String graphId, final String newGraphId, final User requestingUser) throws StorageException {
        return changeGraphId(graphId, newGraphId, access -> access.hasWriteAccess(requestingUser));
    }

    public boolean changeGraphId(final String graphId, final String newGraphId, final User requestingUser, final String adminAuth) throws StorageException {
        return changeGraphId(graphId, newGraphId, access -> access.hasWriteAccess(requestingUser, adminAuth));
    }

    private boolean changeGraphId(final String graphId, final String newGraphId, final Predicate<FederatedAccess> accessPredicate) throws StorageException {
        boolean rtn;
        final GraphSerialisable graphToUpdate = getGraphToUpdate(graphId, accessPredicate);

        if (nonNull(graphToUpdate)) {
            // Get access before removing old graphId.
            FederatedAccess access = federatedStoreCache.getAccessFromCache(graphId);
            // Remove first, to stop a sync issue when sharing the cache with another store.
            remove(graphId, federatedAccess -> true, false);
            // For Accumulo derived stores the table name must be updated to match the graph's new id. These stores are:
            // uk.gov.gchq.gaffer.accumulostore.[AccumuloStore, SingleUseAccumuloStore, MiniAccumuloStore, SingleUseMiniAccumuloStore]
            updateTablesWithNewGraphId(newGraphId, graphToUpdate);

            updateCacheWithNewGraphId(newGraphId, graphToUpdate, access);

            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
    }

    private void updateCacheWithNewGraphId(final String newGraphId, final GraphSerialisable graphToUpdate, final FederatedAccess access) throws StorageException {
        //rename graph
        GraphSerialisable updatedGraphSerialisable = new GraphSerialisable.Builder(graphToUpdate)
                .config(cloneGraphConfigWithNewGraphId(newGraphId, graphToUpdate))
                .build();

        try {
            this.put(updatedGraphSerialisable, access);
        } catch (final Exception e) {
            String s = "Contact Admin for recovery. Error occurred updating graphId. GraphStorage=updated, Cache=outdated graphId.";
            LOGGER.error("{} graphStorage graphId:{} cache graphId:{}", s, newGraphId, graphToUpdate.getGraphId());
            throw new StorageException(s, e);
        }
    }

    private void updateTablesWithNewGraphId(final String newGraphId, final GraphSerialisable graphToUpdate) throws StorageException {
        String graphId = graphToUpdate.getGraphId();
        String storeClass = graphToUpdate.getStoreProperties().getStoreClass();
        if (nonNull(storeClass) && storeClass.startsWith(AccumuloStore.class.getPackage().getName())) {
            try {
                renameTable((AccumuloProperties) graphToUpdate.getStoreProperties(), graphToUpdate.getGraphId(), newGraphId);
            } catch (final Exception e) {
                LOGGER.error("Error trying to update tables for graphID:{} graphUpdate:{}, Error:{}", graphId, graphToUpdate, e.getMessage());
            }
        }
    }

    private GraphConfig cloneGraphConfigWithNewGraphId(final String newGraphId, final GraphSerialisable graphToUpdate) {
        return new GraphConfig.Builder()
                .json(new GraphSerialisable.Builder(graphToUpdate).build().getSerialisedConfig())
                .graphId(newGraphId)
                .build();
    }

    private GraphSerialisable getGraphToUpdate(final String graphId, final Predicate<FederatedAccess> accessPredicate) {
        Pair<GraphSerialisable, FederatedAccess> fromCache = federatedStoreCache.getFromCache(graphId);
        return accessPredicate.test(fromCache.getSecond())
                ? fromCache.getFirst()
                : null;
    }
}
