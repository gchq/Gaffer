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
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class FederatedGraphStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedGraphStorage.class);
    public static final boolean DEFAULT_DISABLED_BY_DEFAULT = false;
    public static final String ERROR_ADDING_GRAPH_TO_CACHE = "Error adding graph, GraphId is known within the cache, but %s is different. GraphId: %s";
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    public static final String ACCESS_IS_NULL = "Can not put graph into storage without a FederatedAccess key.";
    public static final String GRAPH_IDS_NOT_VISIBLE = "The following graphIds are not visible or do not exist: %s";
    private Map<FederatedAccess, Set<Graph>> storage = new HashMap<>();
    private FederatedStoreCache federatedStoreCache = new FederatedStoreCache();
    private Boolean isCacheEnabled = false;
    private GraphLibrary graphLibrary;

    protected void startCacheServiceLoader() throws StorageException {
        if (CacheServiceLoader.isEnabled()) {
            isCacheEnabled = true;
            makeAllGraphsFromCache();
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
                    throw new IllegalArgumentException(ACCESS_IS_NULL);
                }

                if (null != graphLibrary) {
                    graphLibrary.checkExisting(graphId, graph.getDeserialisedSchema(), graph.getDeserialisedProperties());
                }

                validateExisting(graph);
                final Graph builtGraph = graph.getGraph();
                if (isCacheEnabled()) {
                    addToCache(builtGraph, access);
                }

                Set<Graph> existingGraphs = storage.get(access);
                if (null == existingGraphs) {
                    existingGraphs = Sets.newHashSet(builtGraph);
                    storage.put(access, existingGraphs);
                } else {
                    existingGraphs.add(builtGraph);
                }
            } catch (final Exception e) {
                throw new StorageException("Error adding graph " + graphId + " to storage due to: " + e.getMessage(), e);
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
        return getIdsFrom(getUserGraphStream(entry -> entry.getKey().hasReadAccess(user)));
    }

    public Collection<String> getAllIds(final User user, final String adminAuth) {
        return getIdsFrom(getUserGraphStream(entry -> entry.getKey().hasReadAccess(user, adminAuth)));
    }

    private Collection<String> getIdsFrom(final Stream<Graph> allStream) {
        final Set<String> rtn = allStream
                .map(Graph::getGraphId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Collections.unmodifiableSet(rtn);
    }

    /**
     * Returns all graph object that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphs
     */
    public Collection<Graph> getAll(final User user) {
        final Set<Graph> rtn = getUserGraphStream(entry -> entry.getKey().hasReadAccess(user))
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
        return remove(graphId, entry -> entry.getKey().hasWriteAccess(user));
    }

    protected boolean remove(final String graphId, final User user, final String adminAuth) {
        return remove(graphId, entry -> entry.getKey().hasWriteAccess(user, adminAuth));
    }

    private boolean remove(final String graphId, final Predicate<Entry<FederatedAccess, Set<Graph>>> entryPredicateForGraphRemoval) {
        return storage.entrySet().stream()
                .filter(entryPredicateForGraphRemoval)
                .map(entry -> {
                    boolean isRemoved = false;
                    final Set<Graph> graphs = entry.getValue();
                    if (null != graphs) {
                        HashSet<Graph> remove = Sets.newHashSet();
                        for (final Graph graph : graphs) {
                            if (graph.getGraphId().equals(graphId)) {
                                remove.add(graph);
                                deleteFromCache(graphId);
                                isRemoved = true;
                            }
                        }
                        graphs.removeAll(remove);
                    }
                    return isRemoved;
                })
                .collect(Collectors.toSet())
                .contains(true);
    }

    private void deleteFromCache(final String graphId) {
        if (isCacheEnabled()) {
            federatedStoreCache.deleteGraphFromCache(graphId);
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
    public Collection<Graph> get(final User user, final List<String> graphIds) {
        return get(user, graphIds, null);
    }

    /**
     * returns all graphs objects matching the given graphIds, that is visible
     * to the user.
     *
     * @param user      to match visibility against.
     * @param graphIds  the graphIds to get graphs for.
     * @param adminAuth adminAuths role
     * @return visible graphs from the given graphIds.
     */
    public Collection<Graph> get(final User user, final List<String> graphIds, final String adminAuth) {
        if (null == user) {
            return Collections.emptyList();
        }

        validateAllGivenGraphIdsAreVisibleForUser(user, graphIds, adminAuth);
        Stream<Graph> graphs = getStream(user, graphIds);
        if (null != graphIds) {
            graphs = graphs.sorted(Comparator.comparingInt(g -> graphIds.indexOf(g.getGraphId())));
        }
        final Set<Graph> rtn = graphs.collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableCollection(rtn);
    }

    @Deprecated
    public Schema getSchema(final FederatedOperation<Void, Object> operation, final Context context) {
        if (null == context || null == context.getUser()) {
            // no user then return an empty schema
            return new Schema();
        }
        final List<String> graphIds = isNull(operation) ? null : operation.getGraphIds();

        final Stream<Graph> graphs = getStream(context.getUser(), graphIds);
        final Builder schemaBuilder = new Builder();
        try {
            if (nonNull(operation) && operation.hasPayloadOperation() && operation.payloadInstanceOf(GetSchema.class) && ((GetSchema) operation.getPayloadOperation()).isCompact()) {
                graphs.forEach(g -> {
                    try {
                        schemaBuilder.merge(g.execute((GetSchema) operation.getPayloadOperation(), context));
                    } catch (final OperationException e) {
                        throw new RuntimeException("Unable to fetch schema from graph " + g.getGraphId(), e);
                    }
                });
            } else {
                graphs.forEach(g -> schemaBuilder.merge(g.getSchema()));
            }
        } catch (final SchemaException e) {
            final List<String> resultGraphIds = getStream(context.getUser(), graphIds).map(Graph::getGraphId).collect(Collectors.toList());
            throw new SchemaException("Unable to merge the schemas for all of your federated graphs: " + resultGraphIds + ". You can limit which graphs to query for using the FederatedOperation.graphIds.", e);
        }
        return schemaBuilder.build();
    }

    private void validateAllGivenGraphIdsAreVisibleForUser(final User user, final Collection<String> graphIds, final String adminAuth) {
        if (null != graphIds) {
            final Collection<String> visibleIds = getAllIds(user, adminAuth);
            if (!visibleIds.containsAll(graphIds)) {
                final Set<String> notVisibleIds = Sets.newHashSet(graphIds);
                notVisibleIds.removeAll(visibleIds);
                throw new IllegalArgumentException(String.format(GRAPH_IDS_NOT_VISIBLE, notVisibleIds));
            }
        }
    }

    private void validateExisting(final GraphSerialisable graph) throws StorageException {
        final String graphId = graph.getGraphId();
        for (final Set<Graph> graphs : storage.values()) {
            for (final Graph g : graphs) {
                if (g.getGraphId().equals(graphId)) {
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
        return null != access && access.hasReadAccess(user);
    }

    /**
     * @param user     to match visibility against.
     * @param graphIds filter on graphIds
     * @return a stream of graphs for the given graphIds and the user has visibility for.
     * If graphIds is null then only enabled by default graphs are returned that the user can see.
     */
    private Stream<Graph> getStream(final User user, final Collection<String> graphIds) {
        if (isNull(graphIds)) {
            return storage.entrySet()
                    .stream()
                    .filter(entry -> isValidToView(user, entry.getKey()))
                    //not visible unless graphId is requested. //TODO FS disabledByDefault: Review test
                    .filter(entry -> !entry.getKey().isDisabledByDefault())
                    .flatMap(entry -> entry.getValue().stream());
        }

        return storage.entrySet()
                .stream()
                .filter(entry -> isValidToView(user, entry.getKey()))
                .flatMap(entry -> entry.getValue().stream())
                .filter(graph -> graphIds.contains(graph.getGraphId()));
    }

    /**
     * @param readAccessPredicate to filter graphs.
     * @return a stream of graphs the user has visibility for.
     */
    private Stream<Graph> getUserGraphStream(final Predicate<Entry<FederatedAccess, Set<Graph>>> readAccessPredicate) {
        return storage.entrySet()
                .stream()
                .filter(readAccessPredicate)
                .flatMap(entry -> entry.getValue().stream());
    }

    private void addToCache(final Graph newGraph, final FederatedAccess access) {
        final String graphId = newGraph.getGraphId();
        if (federatedStoreCache.contains(graphId)) {
            validateSameAsFromCache(newGraph, graphId);
        } else {
            try {
                federatedStoreCache.addGraphToCache(newGraph, access, false);
            } catch (final OverwritingException e) {
                throw new OverwritingException((String.format("User is attempting to overwrite a graph within the cacheService. GraphId: %s", graphId)));
            } catch (final CacheOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void validateSameAsFromCache(final Graph newGraph, final String graphId) {
        final Graph fromCache = federatedStoreCache.getGraphSerialisableFromCache(graphId).getGraph(graphLibrary);
        if (!newGraph.getStoreProperties().getProperties().equals(fromCache.getStoreProperties().getProperties())) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, GraphConfigEnum.PROPERTIES.toString(), graphId));
        } else {
            if (!JsonUtil.equals(newGraph.getSchema().toJson(false), fromCache.getSchema().toJson(false))) {
                throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, GraphConfigEnum.SCHEMA.toString(), graphId));
            } else {
                if (!newGraph.getGraphId().equals(fromCache.getGraphId())) {
                    throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, "GraphId", graphId));
                }
            }
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

    private void makeGraphFromCache(final String graphId) throws StorageException {
        final GraphSerialisable graph = federatedStoreCache.getGraphSerialisableFromCache(graphId);
        final FederatedAccess accessFromCache = federatedStoreCache.getAccessFromCache(graphId);
        put(graph, accessFromCache);
    }

    private void makeAllGraphsFromCache() throws StorageException {
        final Set<String> allGraphIds = federatedStoreCache.getAllGraphIds();
        for (final String graphId : allGraphIds) {
            try {
                makeGraphFromCache(graphId);
            } catch (final Exception e) {
                LOGGER.error("Skipping graphId: {} due to: {}", graphId, e.getMessage(), e);
            }
        }
    }

    protected Map<String, Object> getAllGraphsAndAccess(final User user, final List<String> graphIds) {
        return getAllGraphsAndAccess(graphIds, access -> access != null && access.hasReadAccess(user));
    }

    protected Map<String, Object> getAllGraphsAndAccessAsAdmin(final User user, final List<String> graphIds, final String adminAuth) {
        return getAllGraphsAndAccess(graphIds, access -> access != null && access.hasReadAccess(user, adminAuth));
    }

    private Map<String, Object> getAllGraphsAndAccess(final List<String> graphIds, final Predicate<FederatedAccess> accessPredicate) {
        return storage.entrySet()
                .stream()
                //filter on FederatedAccess
                .filter(e -> accessPredicate.test(e.getKey()))
                //convert to Map<graphID,FederatedAccess>
                .flatMap(entry -> entry.getValue().stream().collect(Collectors.toMap(Graph::getGraphId, g -> entry.getKey())).entrySet().stream())
                //filter on if graph required?
                .filter(entry -> {
                    final boolean isGraphIdRequested = nonNull(graphIds) && graphIds.contains(entry.getKey());
                    final boolean isAllGraphIdsRequired = isNull(graphIds) || graphIds.isEmpty();
                    return isGraphIdRequested || isAllGraphIdsRequired;
                })
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }


    public boolean changeGraphAccess(final String graphId, final FederatedAccess newFederatedAccess, final User requestingUser) throws StorageException {
        return changeGraphAccess(graphId, newFederatedAccess, access -> access.hasWriteAccess(requestingUser));
    }

    public boolean changeGraphAccess(final String graphId, final FederatedAccess newFederatedAccess, final User requestingUser, final String adminAuth) throws StorageException {
        return changeGraphAccess(graphId, newFederatedAccess, access -> access.hasWriteAccess(requestingUser, adminAuth));
    }

    private boolean changeGraphAccess(final String graphId, final FederatedAccess newFederatedAccess, final Predicate<FederatedAccess> accessPredicate) throws StorageException {
        boolean rtn;
        final Graph graphToMove = getGraphToMove(graphId, accessPredicate);

        if (nonNull(graphToMove)) {
            //remove graph to be moved
            FederatedAccess oldAccess = null;
            for (final Entry<FederatedAccess, Set<Graph>> entry : storage.entrySet()) {
                entry.getValue().removeIf(graph -> graph.getGraphId().equals(graphId));
                oldAccess = entry.getKey();
            }

            //add the graph being moved.
            this.put(new GraphSerialisable.Builder().graph(graphToMove).build(), newFederatedAccess);

            if (isCacheEnabled()) {
                //Update cache
                try {
                    federatedStoreCache.addGraphToCache(graphToMove, newFederatedAccess, true/*true because graphLibrary should have throw error*/);
                } catch (final CacheOperationException e) {
                    String message = String.format("Error occurred updating graphAccess. GraphStorage=updated, Cache=outdated. graphId:%s. Recovery is possible from a restart if a persistent cache is being used, otherwise contact admin", graphId);
                    LOGGER.error("{} graphStorage access:{} cache access:{}", message, newFederatedAccess, oldAccess);
                    throw new StorageException(message, e);
                }
            }

            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
    }

    public boolean changeGraphId(final String graphId, final String newGraphId, final User requestingUser) throws StorageException {
        return changeGraphId(graphId, newGraphId, access -> access.hasWriteAccess(requestingUser));
    }

    public boolean changeGraphId(final String graphId, final String newGraphId, final User requestingUser, final String adminAuth) throws StorageException {
        return changeGraphId(graphId, newGraphId, access -> access.hasWriteAccess(requestingUser, adminAuth));
    }

    private boolean changeGraphId(final String graphId, final String newGraphId, final Predicate<FederatedAccess> accessPredicate) throws StorageException {
        boolean rtn;
        final Graph graphToMove = getGraphToMove(graphId, accessPredicate);

        if (nonNull(graphToMove)) {
            FederatedAccess key = null;
            //remove graph to be moved from storage
            for (final Entry<FederatedAccess, Set<Graph>> entry : storage.entrySet()) {
                final boolean removed = entry.getValue().removeIf(graph -> graph.getGraphId().equals(graphId));
                if (removed) {
                    key = entry.getKey();
                    break;
                }
            }

            //Update Tables
            String storeClass = graphToMove.getStoreProperties().getStoreClass();
            if (nonNull(storeClass) && storeClass.startsWith(AccumuloStore.class.getPackage().getName())) {
                /*
                 * This logic is only for Accumulo derived stores Only.
                 * For updating table names to match graphs names.
                 *
                 * uk.gov.gchq.gaffer.accumulostore.[AccumuloStore, SingleUseAccumuloStore,
                 * MiniAccumuloStore, SingleUseMiniAccumuloStore]
                 */
                try {
                    AccumuloProperties tmpAccumuloProps = (AccumuloProperties) graphToMove.getStoreProperties();
                    Connector connection = TableUtils.getConnector(tmpAccumuloProps.getInstance(),
                            tmpAccumuloProps.getZookeepers(),
                            tmpAccumuloProps.getUser(),
                            tmpAccumuloProps.getPassword());

                    if (connection.tableOperations().exists(graphId)) {
                        connection.tableOperations().offline(graphId);
                        connection.tableOperations().rename(graphId, newGraphId);
                        connection.tableOperations().online(newGraphId);
                    }
                } catch (final Exception e) {
                    LOGGER.warn("Error trying to update tables for graphID:{} graphToMove:{}", graphId, graphToMove);
                    LOGGER.warn("Error trying to update tables.", e);
                }
            }

            final GraphConfig configWithNewGraphId = cloneGraphConfigWithNewGraphId(newGraphId, graphToMove);

            //add the graph being renamed.
            GraphSerialisable newGraphSerialisable = new GraphSerialisable.Builder()
                    .graph(graphToMove)
                    .config(configWithNewGraphId)
                    .build();
            this.put(newGraphSerialisable, key);

            //Update cache
            if (isCacheEnabled()) {
                try {
                    //Overwrite cache = true because the graphLibrary should have thrown an error before this point.
                    federatedStoreCache.addGraphToCache(newGraphSerialisable, key, true);
                } catch (final CacheOperationException e) {
                    String message = "Contact Admin for recovery. Error occurred updating graphId. GraphStorage=updated, Cache=outdated graphId.";
                    LOGGER.error("{} graphStorage graphId:{} cache graphId:{}", message, newGraphId, graphId);
                    throw new StorageException(message, e);
                }
                federatedStoreCache.deleteGraphFromCache(graphId);
            }

            rtn = true;
        } else {
            rtn = false;
        }
        return rtn;
    }

    private GraphConfig cloneGraphConfigWithNewGraphId(final String newGraphId, final Graph graphToMove) {
        return new GraphConfig.Builder()
                .json(new GraphSerialisable.Builder().graph(graphToMove).build().getConfig())
                .graphId(newGraphId)
                .build();
    }

    private Graph getGraphToMove(final String graphId, final Predicate<FederatedAccess> accessPredicate) {
        Graph graphToMove = null;
        for (final Entry<FederatedAccess, Set<Graph>> entry : storage.entrySet()) {
            if (accessPredicate.test(entry.getKey())) {
                //select graph to be moved
                for (final Graph graph : entry.getValue()) {
                    if (graph.getGraphId().equals(graphId)) {
                        if (isNull(graphToMove)) {
                            //1st match, store graph and continue.
                            graphToMove = graph;
                        } else {
                            //2nd match.
                            throw new IllegalStateException("graphIds are unique, but more than one graph was found with the same graphId: " + graphId);
                        }
                    }
                }
            }
        }
        return graphToMove;
    }


}
