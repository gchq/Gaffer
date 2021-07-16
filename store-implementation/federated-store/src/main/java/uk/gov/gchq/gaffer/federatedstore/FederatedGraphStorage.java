/*
 * Copyright 2017-2021 Crown Copyright
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
import org.apache.accumulo.core.client.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.CACHE_SERVICE_CLASS;

public class FederatedGraphStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedGraphStorage.class);
    public static final boolean DEFAULT_DISABLED_BY_DEFAULT = false;
    public static final String ERROR_ADDING_GRAPH_TO_CACHE = "Error adding graph, GraphId is known within the cache, but %s is different. GraphId: %s";
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    public static final String ACCESS_IS_NULL = "Can not put graph into storage without a FederatedAccess key.";
    public static final String GRAPH_IDS_NOT_VISIBLE = "The following graphIds are not visible or do not exist: %s";
    public static final String UNABLE_TO_MERGE_THE_SCHEMAS_FOR_ALL_OF_YOUR_FEDERATED_GRAPHS = "Unable to merge the schemas for all of your federated graphs: %s. You can limit which graphs to query for using the operation option: %s";
    private FederatedStoreCache federatedStoreCache;
    private GraphLibrary graphLibrary;

    public FederatedGraphStorage() {
        this(null);
    }

    public FederatedGraphStorage(final String cacheNameSuffix) {
        federatedStoreCache = new FederatedStoreCache(cacheNameSuffix);
    }

    protected void startCacheServiceLoader() throws StorageException {
        if (!CacheServiceLoader.isEnabled()) {
            throw new StorageException("Cache is not enabled for the FederatedStore, Set a value in StoreProperties for " + CACHE_SERVICE_CLASS);
        }
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
     * <p> GraphId can't already exist, otherwise {@link
     * OverwritingException} is thrown.
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
                    graphLibrary.checkExisting(graphId, graph.getDeserialisedSchema(graphLibrary), graph.getDeserialisedProperties(graphLibrary));
                }

                validateExisting(graphId);

                addToCache(graph, access);

            } catch (final Exception e) {
                throw new StorageException("Error adding graph " + graphId + (nonNull(e.getMessage()) ? (" to storage due to: " + e.getMessage()) : "."), e);
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
    public Collection<String> getAllIds(final User user) {
        return getIdsFrom(getUserGraphStream(federatedAccess -> federatedAccess.hasReadAccess(user)));
    }

    public Collection<String> getAllIds(final User user, final String adminAuth) {
        return getIdsFrom(getUserGraphStream(federatedAccess -> federatedAccess.hasReadAccess(user, adminAuth)));
    }

    @Deprecated
    protected Collection<String> getAllIdsAsAdmin() {
        return federatedStoreCache.getAllGraphIds();
    }

    private Collection<String> getIdsFrom(final Stream<GraphSerialisable> allStream) {
        final Set<String> rtn = allStream
                .map(GraphSerialisable::getGraphId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Collections.unmodifiableSet(rtn);
    }

    /**
     * Returns all graph object that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphs
     */
    public Collection<GraphSerialisable> getAll(final User user) {
        final Set<GraphSerialisable> rtn = getUserGraphStream(federatedAccess -> federatedAccess.hasReadAccess(user))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableCollection(rtn);
    }

    /**
     * Removes a graph from storage and returns the success. The given user
     * must
     * have visibility of the graph to be able to remove it.
     *
     * @param graphId the graphId to remove.
     * @param user    to match visibility against.
     * @return if a graph was removed.
     * @see #isValidToView(User, FederatedAccess)
     */
    public boolean remove(final String graphId, final User user) {
        return remove(graphId, federatedAccess -> federatedAccess.hasWriteAccess(user));
    }

    @Deprecated
    protected boolean remove(final String graphId) {
        return remove(graphId, federatedAccess -> true);
    }

    protected boolean remove(final String graphId, final User user, final String adminAuth) {
        return remove(graphId, federatedAccess -> federatedAccess.hasWriteAccess(user, adminAuth));
    }

    private boolean remove(final String graphId, final Predicate<FederatedAccess> accessPredicate) {
        FederatedAccess accessFromCache = federatedStoreCache.getAccessFromCache(graphId);
        boolean rtn;
        if (nonNull(accessFromCache) ? accessPredicate.test(accessFromCache) : false) {
            federatedStoreCache.deleteFromCache(graphId);
            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
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
        if (null == user) {
            return Collections.emptyList();
        }

        validateAllGivenGraphIdsAreVisibleForUser(user, graphIds);
        Stream<GraphSerialisable> graphs = getStream(user, graphIds);
        if (null != graphIds) {
            graphs = graphs.sorted(Comparator.comparingInt(g -> graphIds.indexOf(g.getGraphId())));
        }
        final Set<GraphSerialisable> rtn = graphs.collect(Collectors.toCollection(LinkedHashSet::new));
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

        final List<String> graphIds = FederatedStoreUtil.getGraphIds(operation.getOptions());
        final Stream<GraphSerialisable> graphs = getStream(context.getUser(), graphIds);
        final Builder schemaBuilder = new Builder();
        try {
            if (operation.isCompact()) {
                final GetSchema getSchema = new GetSchema.Builder()
                        .compact(true)
                        .build();
                graphs.forEach(gs -> {
                    try {
                        schemaBuilder.merge(gs.getGraph().execute(getSchema, context));
                    } catch (final Exception e) {
                        throw new GafferRuntimeException("Unable to fetch schema from graph " + gs.getGraphId(), e);
                    }
                });
            } else {
                graphs.forEach(g -> schemaBuilder.merge(g.getDeserialisedSchema(graphLibrary)));
            }
        } catch (final SchemaException e) {
            final List<String> resultGraphIds = getStream(context.getUser(), graphIds).map(GraphSerialisable::getGraphId).collect(Collectors.toList());
            throw new SchemaException("Unable to merge the schemas for all of your federated graphs: " + resultGraphIds + ". You can limit which graphs to query for using the operation option: " + KEY_OPERATION_OPTIONS_GRAPH_IDS, e);
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

        final List<String> graphIds = FederatedStoreUtil.getGraphIds(config);
        final Collection<GraphSerialisable> graphs = get(user, graphIds);
        final Builder schemaBuilder = new Builder();
        try {
            graphs.forEach(g -> schemaBuilder.merge(g.getDeserialisedSchema(graphLibrary)));
        } catch (final SchemaException e) {
            final List<String> resultGraphIds = getStream(user, graphIds).map(GraphSerialisable::getGraphId).collect(Collectors.toList());
            throw new SchemaException(String.format(UNABLE_TO_MERGE_THE_SCHEMAS_FOR_ALL_OF_YOUR_FEDERATED_GRAPHS, resultGraphIds, KEY_OPERATION_OPTIONS_GRAPH_IDS), e);
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
     * @deprecated use {@link uk.gov.gchq.gaffer.store.Store#execute(uk.gov.gchq.gaffer.operation.Operation, Context)} with GetTraits Operation.
     */
    @Deprecated
    public Set<StoreTrait> getTraits(final GetTraits op, final Context context) {
        boolean firstPass = true;
        final Set<StoreTrait> traits = new HashSet<>();
        if (null != op) {
            final List<String> graphIds = FederatedStoreUtil.getGraphIds(op.getOptions());
            final Collection<GraphSerialisable> graphs = get(context.getUser(), graphIds);
            final GetTraits getTraits = op.shallowClone();
            for (final GraphSerialisable graph : graphs) {
                try {

                    Set<StoreTrait> execute = graph.getGraph().execute(getTraits, context);
                    if (firstPass) {
                        traits.addAll(execute);
                        firstPass = false;
                    } else {
                        traits.retainAll(execute);
                    }
                } catch (final Exception e) {
                    throw new RuntimeException("Unable to fetch traits from graph " + graph.getGraphId(), e);
                }
            }
        }

        return traits;
    }

    private void validateAllGivenGraphIdsAreVisibleForUser(final User user, final Collection<String> graphIds) {
        if (nonNull(graphIds)) {
            final Collection<String> visibleIds = getAllIds(user);
            if (!visibleIds.containsAll(graphIds)) {
                final Set<String> notVisibleIds = Sets.newHashSet(graphIds);
                notVisibleIds.removeAll(visibleIds);
                throw new IllegalArgumentException(String.format(GRAPH_IDS_NOT_VISIBLE, notVisibleIds));
            }
        }
    }

    private void validateExisting(final String graphId) throws StorageException {
        boolean exists = federatedStoreCache.getAllGraphIds().contains(graphId);
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
        return isNull(graphIds)
                ? federatedStoreCache.getAllGraphIds().stream()
                .map(g -> federatedStoreCache.getFromCache(g))
                .filter(pair -> isValidToView(user, pair.getSecond()))
                .filter(pair -> !pair.getSecond().isDisabledByDefault())
                .map(pair -> pair.getFirst())

                : federatedStoreCache.getAllGraphIds().stream()
                .map(g -> federatedStoreCache.getFromCache(g))
                .filter(pair -> isValidToView(user, pair.getSecond()))
                .filter(pair -> graphIds.contains(pair.getFirst().getGraphId()))
                .map(pair -> pair.getFirst());
    }

    /**
     * @param readAccessPredicate to filter graphs.
     * @return a stream of graphs the user has visibility for.
     */
    private Stream<GraphSerialisable> getUserGraphStream(final Predicate<FederatedAccess> readAccessPredicate) {
        return federatedStoreCache.getAllGraphIds().stream()
                .map(graphId -> federatedStoreCache.getFromCache(graphId))
                .filter(pair -> readAccessPredicate.test(pair.getSecond()))
                .map(Pair::getFirst);
    }

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

        if (!newGraph.getDeserialisedProperties(graphLibrary).getProperties().equals(fromCache.getDeserialisedProperties(graphLibrary))) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, GraphConfigEnum.PROPERTIES.toString(), graphId));
        }
        if (!JsonUtil.equals(newGraph.getDeserialisedSchema(graphLibrary).toJson(false), fromCache.getDeserialisedSchema(graphLibrary).toJson(false))) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, GraphConfigEnum.SCHEMA.toString(), graphId));
        }
        if (!newGraph.getGraphId().equals(fromCache.getGraphId())) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, "GraphId", graphId));
        }
    }

    public void setGraphLibrary(final GraphLibrary graphLibrary) {
        this.graphLibrary = graphLibrary;
    }

    /**
     * Enum for the Graph Properties or Schema
     */
    public enum GraphConfigEnum {
        SCHEMA("schema"), PROPERTIES("properties");

        private final String value;

        GraphConfigEnum(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

    }

    protected Map<String, Object> getAllGraphsAndAccess(final User user, final List<String> graphIds) {
        return getAllGraphsAndAccess(graphIds, access -> access != null && access.hasReadAccess(user));
    }

    protected Map<String, Object> getAllGraphsAndAccess(final User user, final List<String> graphIds, final String adminAuth) {
        return getAllGraphsAndAccess(graphIds, access -> access != null && access.hasReadAccess(user, adminAuth));
    }

    @Deprecated
    protected Map<String, Object> getAllGraphAndAccessAsAdmin(final List<String> graphIds) {
        return getAllGraphsAndAccess(graphIds, entry -> true);
    }

    private Map<String, Object> getAllGraphsAndAccess(final List<String> graphIds, final Predicate<FederatedAccess> accessPredicate) {
        return federatedStoreCache.getAllGraphIds().stream()
                .map(graphId -> federatedStoreCache.getFromCache(graphId))
                //filter on FederatedAccess
                .filter(pair -> accessPredicate.test(pair.getSecond()))
                //filter on if graph required?
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

    @Deprecated
    public boolean changeGraphAccessAsAdmin(final String graphId, final FederatedAccess newFederatedAccess) throws StorageException {
        return changeGraphAccess(graphId, newFederatedAccess, access -> true);
    }

    private boolean changeGraphAccess(final String graphId, final FederatedAccess newFederatedAccess, final Predicate<FederatedAccess> accessPredicate) throws StorageException {
        boolean rtn;
        final GraphSerialisable graphToMove = getGraphToMove(graphId, accessPredicate);

        if (nonNull(graphToMove)) {
            //remove graph to be moved
            remove(graphId, federatedAccess -> true);

            updateCacheWithNewAccess(graphId, newFederatedAccess, graphToMove);

            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
    }

    private void updateCacheWithNewAccess(final String graphId, final FederatedAccess newFederatedAccess, final GraphSerialisable graphToMove) throws StorageException {
        try {
            this.put(new GraphSerialisable.Builder(graphToMove).build(), newFederatedAccess);
        } catch (final Exception e) {
            //TODO FS recovery
            String s = "Error occurred updating graphAccess. GraphStorage=updated, Cache=outdated. graphId:" + graphId;
            LOGGER.error(s + " graphStorage access:{} cache access:{}", newFederatedAccess, federatedStoreCache.getAccessFromCache(graphId));
            throw new StorageException(s, e);
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
        final GraphSerialisable graphToMove = getGraphToMove(graphId, accessPredicate);

        if (nonNull(graphToMove)) {
            //get access before removing old graphId.
            FederatedAccess access = federatedStoreCache.getAccessFromCache(graphId);
            //Removed first, to stop a sync issue when sharing the cache with another store.
            remove(graphId, federatedAccess -> true);

            updateTablesWithNewGraphId(newGraphId, graphToMove);

            updateCacheWithNewGraphId(newGraphId, graphToMove, access);

            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
    }

    private void updateCacheWithNewGraphId(final String newGraphId, final GraphSerialisable graphToMove, final FederatedAccess access) throws StorageException {
        //rename graph
        GraphSerialisable updatedGraphSerialisable = new GraphSerialisable.Builder(graphToMove)
                .config(cloneGraphConfigWithNewGraphId(newGraphId, graphToMove))
                .build();

        try {
            this.put(updatedGraphSerialisable, access);
        } catch (final Exception e) {
            //TODO FS recovery
            String s = "Error occurred updating graphId. GraphStorage=updated, Cache=outdated graphId.";
            LOGGER.error(s + " graphStorage graphId:{} cache graphId:{}", newGraphId, graphToMove.getGraphId());
            throw new StorageException(s, e);
        }
    }

    private void updateTablesWithNewGraphId(final String newGraphId, final GraphSerialisable graphToMove) {
        //Update Tables
        String graphId = graphToMove.getGraphId();
        String storeClass = graphToMove.getDeserialisedProperties(graphLibrary).getStoreClass();
        if (nonNull(storeClass) && storeClass.startsWith(AccumuloStore.class.getPackage().getName())) {
            /*
             * uk.gov.gchq.gaffer.accumulostore.[AccumuloStore, SingleUseAccumuloStore,
             * SingleUseMockAccumuloStore, MockAccumuloStore, MiniAccumuloStore]
             */
            try {
                AccumuloProperties tmpAccumuloProps = (AccumuloProperties) graphToMove.getDeserialisedProperties();
                Connector connection = TableUtils.getConnector(tmpAccumuloProps.getInstance(),
                        tmpAccumuloProps.getZookeepers(),
                        tmpAccumuloProps.getUser(),
                        tmpAccumuloProps.getPassword());

                if (connection.tableOperations().exists(graphId)) {
                    connection.tableOperations().offline(graphId);
                    connection.tableOperations().clone(graphId, newGraphId, true, null, null);
                    connection.tableOperations().online(newGraphId);
                    connection.tableOperations().delete(graphId);
                }
            } catch (final Exception e) {
                LOGGER.warn(String.format("Error trying to update tables for graphID:%s graphToMove:%s", graphId, graphToMove), e);
            }
        }
    }

    private GraphConfig cloneGraphConfigWithNewGraphId(final String newGraphId, final GraphSerialisable graphToMove) {
        return new GraphConfig.Builder()
                .json(new GraphSerialisable.Builder(graphToMove).build().getConfig())
                .graphId(newGraphId)
                .build();
    }

    private GraphSerialisable getGraphToMove(final String graphId, final Predicate<FederatedAccess> accessPredicate) {
        Pair<GraphSerialisable, FederatedAccess> fromCache = federatedStoreCache.getFromCache(graphId);
        return accessPredicate.test(fromCache.getSecond())
                ? fromCache.getFirst()
                : null;
    }
}
