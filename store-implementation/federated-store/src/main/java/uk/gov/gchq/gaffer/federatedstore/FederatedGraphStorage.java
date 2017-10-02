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
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FederatedGraphStorage {
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    public static final String CAN_NOT_PUT_GRAPH_INTO_STORAGE_WITHOUT_A_FEDERATED_ACCESS_KEY = "Can not put graph into storage without a FederatedAccess key.";
    private Map<FederatedAccess, Set<Graph>> storage = new HashMap<>();
    private Schema mergedSchema = new Schema();
    private Set<StoreTrait> mergedTraits = Collections.emptySet();

    public Collection<Graph> getAll(final User user) {
        final HashSet<Graph> rtn = storage.entrySet()
                .stream()
                .filter(entry -> null != entry.getKey() && entry.getKey().isValidToExecute(user))
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toCollection(HashSet::new));

        return Collections.unmodifiableCollection(rtn);
    }

    public Collection<Graph> get(final User user, final Collection<String> graphIds) {
        final HashSet<Graph> rtn = getAll(user)
                .stream()
                .filter(graph -> null == graphIds || graphIds.contains(graph.getGraphId()))
                .collect(Collectors.toCollection(HashSet::new));

        return Collections.unmodifiableSet(rtn);
    }


    public Collection<String> getAllIds(final User user) {
        final HashSet<String> rtn = getAll(user)
                .stream()
                .map(Graph::getGraphId)
                .collect(Collectors.toCollection(HashSet::new));

        return Collections.unmodifiableSet(rtn);
    }

    public void put(final Collection<Graph> graphs, final FederatedAccess access) {
        for (final Graph graph : graphs) {
            put(graph, access);
        }
    }

    public void put(final Graph graph, final FederatedAccess access) {
        if (exists(graph.getGraphId())) {
            throw new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S, graph.getGraphId())));
        } else if (null == access) {
            throw new RuntimeException(CAN_NOT_PUT_GRAPH_INTO_STORAGE_WITHOUT_A_FEDERATED_ACCESS_KEY);
        }

        // Check the schema can be merged before adding the graph.
        getMergedSchema(Optional.of(graph));

        Set<Graph> existingGraphs = storage.get(access);
        if (null == existingGraphs) {
            existingGraphs = Sets.newHashSet(graph);
            storage.put(access, existingGraphs);
        } else {
            existingGraphs.add(graph);
        }

        mergedSchema = getMergedSchema(Optional.empty());
        mergedTraits = getMergedTraits(Optional.empty());
    }

    public void remove(final String graphId) {
        for (final Entry<FederatedAccess, Set<Graph>> entry : storage.entrySet()) {
            final Set<Graph> graphs = entry.getValue();
            if (null != graphs) {
                for (final Graph graph : graphs) {
                    if (graph.getGraphId().equals(graphId)) {
                        graphs.remove(graph);
                    }
                }
            }
        }
        mergedSchema = getMergedSchema(Optional.empty());
        mergedTraits = getMergedTraits(Optional.empty());
    }

    public Schema getMergedSchema() {
        return mergedSchema.clone();
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

    public Set<StoreTrait> getTraits() {
        return Collections.unmodifiableSet(mergedTraits);
    }

    private Schema getMergedSchema(final Optional<Graph> newGraph) {
        Builder schemaBuilder = getMergedSchemaBuilder(Optional.empty());

        if (newGraph.isPresent()) {
            schemaBuilder.merge(newGraph.get().getSchema());
//        An exception would be thrown here if something was wrong merging the schema.
        }

        return schemaBuilder.build();
    }

    private Builder getMergedSchemaBuilder(final Optional<User> user) {
        Builder schemaBuilder = new Builder();
        storage.entrySet()
                .stream()
                .filter(filterByOptionalUser(user))
                .flatMap(entry -> entry.getValue().stream()).forEach(graph -> schemaBuilder.merge(graph.getSchema()));
        return schemaBuilder;
    }

    public Set<StoreTrait> getTraits(final User user) {
        return getMergedTraits(Optional.of(user));
    }

    private Set<StoreTrait> getMergedTraits(final Optional<User> user) {
        final HashSet<Graph> graphs = storage.entrySet()
                .stream()
                .filter(filterByOptionalUser(user))
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toCollection(HashSet::new));

        final Set<StoreTrait> newTraits = graphs.isEmpty() ? Sets.newHashSet() : Sets.newHashSet(StoreTrait.values());
        for (final Graph graph : graphs) {
            newTraits.retainAll(graph.getStoreTraits());
        }
        return newTraits;
    }

    private Predicate<Entry<FederatedAccess, Set<Graph>>> filterByOptionalUser(final Optional<User> user) {
        return entry -> {
            final boolean rtn;
            if (user.isPresent()) {
                rtn = entry.getKey().isValidToExecute(user.get());
            } else {
                return true;
            }
            return rtn;
        };
    }

    public Schema getMergedSchema(final User user) {
        return getMergedSchemaBuilder(Optional.of(user)).build();
    }
}
