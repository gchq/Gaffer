/*
 * Copyright 2017 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
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

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedGraphStorage {
    public static final String ERROR_ADDING_GRAPH_TO_CACHE = "Error adding graph, GraphId is known within the cache, but %s is different. GraphId: %s";
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    public static final String ACCESS_IS_NULL = "Can not put graph into storage without a FederatedAccess key.";
    public static final String GRAPH_IDS_NOT_VISIBLE = "The following graphIds are not visible or do not exist: %s";
    public static final String UNABLE_TO_MERGE_THE_SCHEMAS_FOR_ALL_OF_YOUR_FEDERATED_GRAPHS = "Unable to merge the schemas for all of your federated graphs: %s. You can limit which graphs to query for using the operation option: %s";
    public static final String ERROR_SERIALISING_GRAPH = "Error serialising Graph";
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
     * @see #put(Graph, FederatedAccess)
     */
    public void put(final Collection<Graph> graphs, final FederatedAccess access) throws StorageException {
        for (final Graph graph : graphs) {
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
    public void put(final Graph graph, final FederatedAccess access) throws StorageException {
        if (graph != null) {
            String graphId = graph.getGraphId();
            try {
                if (null == access) {
                    throw new IllegalArgumentException(ACCESS_IS_NULL);
                }

                if (null != graphLibrary) {
                    graphLibrary.checkExisting(graphId, graph.getSchema(), graph.getStoreProperties());
                }

                if (!exists(graph, access)) {
                    if (isCacheEnabled()) {
                        addToCache(graph, access);
                    }

                    Set<Graph> existingGraphs = storage.get(access);
                    if (null == existingGraphs) {
                        existingGraphs = Sets.newHashSet(graph);
                        storage.put(access, existingGraphs);
                    } else {
                        existingGraphs.add(graph);
                    }
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
        final Set<String> rtn = getAllStream(user)
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
        final Set<Graph> rtn = getAllStream(user)
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
        boolean isRemoved = false;
        for (final Entry<FederatedAccess, Set<Graph>> entry : storage.entrySet()) {
            if (isValidToView(user, entry.getKey())) {
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
            }
        }
        return isRemoved;
    }

    private void deleteFromCache(final String graphId) {
        if (isCacheEnabled()) {
            federatedStoreCache.deleteFromCache(graphId);
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
        if (null == user) {
            return Collections.emptyList();
        }

        validateAllGivenGraphIdsAreVisibleForUser(user, graphIds);
        Stream<Graph> graphs = getStream(user, graphIds);
        if (null != graphIds) {
            graphs = graphs.sorted((g1, g2) -> graphIds.indexOf(g1.getGraphId()) - graphIds.indexOf(g2.getGraphId()));
        }
        final Set<Graph> rtn = graphs.collect(Collectors.toCollection(LinkedHashSet::new));
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
        final Stream<Graph> graphs = getStream(context.getUser(), graphIds);
        final Builder schemaBuilder = new Builder();
        try {
            if (operation.isCompact()) {
                final GetSchema getSchema = new GetSchema.Builder()
                        .compact(true)
                        .build();
                graphs.forEach(g -> {
                    try {
                        schemaBuilder.merge(g.execute(getSchema, context));
                    } catch (final OperationException e) {
                        throw new RuntimeException("Unable to fetch schema from graph " + g.getGraphId(), e);
                    }
                });
            } else {
                graphs.forEach(g -> schemaBuilder.merge(g.getSchema()));
            }
        } catch (final SchemaException e) {
            final List<String> resultGraphIds = getStream(context.getUser(), graphIds).map(Graph::getGraphId).collect(Collectors.toList());
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
        final Stream<Graph> graphs = getStream(user, graphIds);
        final Builder schemaBuilder = new Builder();
        try {
            graphs.forEach(g -> schemaBuilder.merge(g.getSchema()));
        } catch (final SchemaException e) {
            final List<String> resultGraphIds = getStream(user, graphIds).map(Graph::getGraphId).collect(Collectors.toList());
            throw new SchemaException(String.format(UNABLE_TO_MERGE_THE_SCHEMAS_FOR_ALL_OF_YOUR_FEDERATED_GRAPHS, resultGraphIds, KEY_OPERATION_OPTIONS_GRAPH_IDS), e);
        }
        return schemaBuilder.build();
    }

    private void validateAllGivenGraphIdsAreVisibleForUser(final User user, final Collection<String> graphIds) {
        if (null != graphIds) {
            final Collection<String> visibleIds = getAllIds(user);
            if (!visibleIds.containsAll(graphIds)) {
                final Set<String> notVisibleIds = Sets.newHashSet(graphIds);
                notVisibleIds.removeAll(visibleIds);
                throw new IllegalArgumentException(String.format(GRAPH_IDS_NOT_VISIBLE, notVisibleIds));
            }
        }
    }

    private boolean exists(final Graph graph, final FederatedAccess access) throws StorageException {
        Graph found = null;
        String graphId = graph.getGraphId();
        outer:
        for (final Set<Graph> graphs : storage.values()) {
            for (final Graph g : graphs) {
                if (g.getGraphId().equals(graphId)) {
                    found = g;
                    break outer;
                }
            }
        }

        boolean rtn = false;

        if (null != found) {
            byte[] graphJson;
            byte[] existJson;
            try {
                graphJson = JSONSerialiser.serialise(graph);
                existJson = JSONSerialiser.serialise(found);
            } catch (final SerialisationException e) {
                throw new StorageException(ERROR_SERIALISING_GRAPH);
            }
            if (JsonUtil.equals(graphJson, existJson)) {
                rtn = true;
            } else {
                throw new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, graphId)));
            }

            Set<Graph> graphs = storage.get(access);
            boolean foundAccess = false;
            if (null != graphs) {
                for (final Graph g : graphs) {
                    if (g.getGraphId().equals(graphId)) {
                        foundAccess = true;
                        break;
                    }
                }
                if (!foundAccess) {
                    throw new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, graphId)));
                }
            } else {
                throw new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, graphId)));
            }
        }
        return rtn;
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
     * @param user     to match visibility against
     * @param graphIds filter on graphIds
     * @return graphs that match graphIds and the user has visibility of.
     */
    private Stream<Graph> getStream(final User user, final Collection<String> graphIds) {
        final Stream<Graph> allStream = getAllStream(user);
        if (null == graphIds) {
            return allStream;
        }

        return allStream.filter(graph -> graphIds.contains(graph.getGraphId()));
    }

    /**
     * @param user to match visibility against.
     * @return a stream of graphs the user has visibility for.
     */
    private Stream<Graph> getAllStream(final User user) {
        return storage.entrySet()
                .stream()
                .filter(entry -> isValidToView(user, entry.getKey()))
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
        final GraphSerialisable serialisable = federatedStoreCache.getGraphSerialisableFromCache(graphId);
        final Graph graph = serialisable.getGraph(graphLibrary);
        final FederatedAccess accessFromCache = federatedStoreCache.getAccessFromCache(graphId);
        put(graph, accessFromCache);
    }

    private void makeAllGraphsFromCache() throws StorageException {
        final Set<String> allGraphIds = federatedStoreCache.getAllGraphIds();
        for (final String graphId : allGraphIds) {
            makeGraphFromCache(graphId);
        }
    }
}
