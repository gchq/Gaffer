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

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FederatedGraphStorage {
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    public static final String ACCESS_IS_NULL = "Can not put graph into storage without a FederatedAccess key.";
    public static final String GRAPH_IDS_NOT_VISIBLE = "The following graphIds are not visible or do not exist: %s";
    private Map<FederatedAccess, Set<Graph>> storage = new HashMap<>();
    @Deprecated
    private Schema mergedSchema = new Schema();
    @Deprecated
    private Set<StoreTrait> mergedTraits = Collections.emptySet();

    /**
     * places a collections of graphs into storage, protected by the given
     * access.
     *
     * @param graphs the graphs to add to the storage.
     * @param access access required to for the graphs, can't be null
     * @see #put(Graph, FederatedAccess)
     */
    public void put(final Collection<Graph> graphs, final FederatedAccess access) {
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
     */
    public void put(final Graph graph, final FederatedAccess access) {
        if (exists(graph.getGraphId())) {
            throw new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, graph.getGraphId())));
        } else if (null == access) {
            throw new IllegalArgumentException(ACCESS_IS_NULL);
        }

        // Check the schema can be merged before adding the graph.
        getMergedSchemaWithGraph(graph);

        Set<Graph> existingGraphs = storage.get(access);
        if (null == existingGraphs) {
            existingGraphs = Sets.newHashSet(graph);
            storage.put(access, existingGraphs);
        } else {
            existingGraphs.add(graph);
        }

        mergedSchema = getMergedSchemaWithGraph(null);
        mergedTraits = getStoreTraits(null);
    }

    /**
     * Returns all the graphIds that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphIds.
     * @see #filterByUserVisibilityHardAccess(User)
     */
    public Collection<String> getAllIds(final User user) {
        final Set<String> rtn = getAllStream(user)
                .map(Graph::getGraphId)
                .collect(Collectors.toSet());

        return Collections.unmodifiableSet(rtn);
    }

    /**
     * Returns all graph object that are visible for the given user.
     *
     * @param user to match visibility against.
     * @return visible graphs
     * @see #filterByUserVisibilityHardAccess(User)
     */
    public Collection<Graph> getAll(final User user) {
        final Set<Graph> rtn = getAllStream(user)
                .collect(Collectors.toSet());
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
                    for (final Graph graph : graphs) {
                        if (graph.getGraphId().equals(graphId)) {
                            graphs.remove(graph);
                            isRemoved = true;
                        }
                    }
                }
            }
        }
        mergedSchema = getMergedSchemaWithGraph(null);

        mergedTraits = getStoreTraits(null);
        return isRemoved;
    }

    /**
     * returns all graphs objects matching the given graphIds, that is visible
     * to the user.
     *
     * @param user     to match visibility against.
     * @param graphIds the graphIds to get graphs for.
     * @return visible graphs from the given graphIds.
     * @see #filterByUserVisibilityHardAccess(User)
     */
    public Collection<Graph> get(final User user, final Collection<String> graphIds) {
        validateAllGivenGraphIdsAreVisibleForUser(user, graphIds);

        final Set<Graph> rtn = getStream(user, graphIds)
                .collect(Collectors.toSet());
        return Collections.unmodifiableCollection(Collections.unmodifiableCollection(rtn));
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


    /**
     * @return merged traits of the entire storage.
     * @deprecated traits should not be visible for graphs a user can not see.
     */
    @Deprecated
    public Set<StoreTrait> getTraits() {
        return Collections.unmodifiableSet(mergedTraits);
    }

    /**
     * @return merged schema of the entire storage.
     * @deprecated schema should not be visible for graphs a user can not see.
     */
    @Deprecated
    public Schema getMergedSchema() {
        return mergedSchema.clone();
    }

    /**
     * @param user to match visibility against.
     * @return merged traits of the visible graphs.
     * @see #filterByUserVisibilityHardAccess(User)
     */
    public Set<StoreTrait> getTraits(final User user) {
        return getStoreTraits(filterByUserVisibilityHardAccess(user));
    }

    /**
     * @param user to match visibility against.
     * @return merged schema of the visible graphs.
     * @see #filterByUserVisibilityHardAccess(User)
     */
    public Schema getMergedSchema(final User user) {
        Builder schemaBuilder = getMergedSchemaBuilder(filterByUserVisibilityHardAccess(user));
        return schemaBuilder.build();
    }

    private boolean exists(final String graphId) {
        boolean exists = false;
        outer:
        for (final Set<Graph> graphs : storage.values()) {
            for (final Graph graph : graphs) {
                if (graph.getGraphId().equals(graphId)) {
                    exists = true;
                    break outer;
                }
            }
        }
        return exists;
    }

    private Schema getMergedSchemaWithGraph(final Graph newGraph) {
        Builder schemaBuilder = getMergedSchemaBuilder(null);

        if (null != newGraph) {
            schemaBuilder.merge(newGraph.getSchema());
//        An exception would be thrown here if something was wrong merging the schema.
        }

        return schemaBuilder.build();
    }

    /**
     * @param user     to match visibility against
     * @param graphIds filter on graphIds
     * @return graphs that match graphIds and the user has visibility of.
     * @see #filterByUserVisibilityHardAccess(User)
     */
    private Stream<Graph> getStream(final User user, final Collection<String> graphIds) {
        return getAllStream(user)
                .filter(graph -> null == graphIds || graphIds.contains(graph.getGraphId()));
    }

    /**
     * @param user to match visibility against, if null will default to
     *             false/denied
     *             access
     * @return the boolean access
     * @see #isValidToView(User, FederatedAccess)
     */
    private Predicate<Entry<FederatedAccess, Set<Graph>>> filterByUserVisibilityHardAccess(final User user) {
        return entry -> isValidToView(user, entry.getKey());
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

    private Builder getMergedSchemaBuilder(final Predicate<Entry<FederatedAccess, Set<Graph>>> userAccessPredicate) {
        Builder schemaBuilder = new Builder();
        getGraphStream(userAccessPredicate)
                .forEach(graph -> schemaBuilder.merge(graph.getSchema()));
        return schemaBuilder;
    }

    /**
     * @param user to match visibility against.
     * @return a stream of graphs the user has visibility for.
     * @see #filterByUserVisibilityHardAccess(User)
     */
    private Stream<Graph> getAllStream(final User user) {
        return storage.entrySet()
                .stream()
                .filter(filterByUserVisibilityHardAccess(user))
                .flatMap(entry -> entry.getValue().stream());
    }

    private Set<StoreTrait> getStoreTraits(final Predicate<Entry<FederatedAccess, Set<Graph>>> userAccessPredicate) {
        final HashSet<Graph> graphs = getGraphStream(userAccessPredicate)
                .collect(Collectors.toCollection(HashSet::new));

        final Set<StoreTrait> newTraits = graphs.isEmpty() ? Sets.newHashSet() : Sets.newHashSet(StoreTrait.values());
        for (final Graph graph : graphs) {
            newTraits.retainAll(graph.getStoreTraits());
        }
        return newTraits;
    }

    private Stream<Graph> getGraphStream(final Predicate<Entry<FederatedAccess, Set<Graph>>> userAccessPredicate) {
        Stream<Entry<FederatedAccess, Set<Graph>>>
                stream = storage.entrySet().stream();

        stream = (null == userAccessPredicate)
                ? stream
                : stream.filter(userAccessPredicate);

        return stream.flatMap(entry -> entry.getValue().stream());
    }
}
