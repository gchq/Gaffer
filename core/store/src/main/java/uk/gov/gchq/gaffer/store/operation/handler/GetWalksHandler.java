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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.Walk;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class GetWalksHandler implements OutputOperationHandler<GetWalks, Iterable<Walk>> {

    private final Set<Object> visitedSeeds = new HashSet<>();
    private int pathVertices;
    private int hops;

    @Override
    public Iterable<Walk> doOperation(final GetWalks operation, final Context context, final Store store) throws OperationException {

        // Check input
        if (null == operation.getInput()) {
            return null;
        }

        // Check operations input
        final Iterable<GetElements> operations = operation.getOperations();

        if (null == operations || Iterables.isEmpty(operations)) {
            return new EmptyClosableIterable<>();
        }

        Iterable<Object> seeds = Streams.toStream(operation.getInput())
                .map(EntitySeed::getVertex)
                .collect(GafferCollectors.toWrappedCloseableIterable());

        hops = Iterables.size(operation.getOperations());
        pathVertices = hops + 1;

        final HashBasedTable<Object, Object, Set<Edge>> edgeTable = HashBasedTable.create();

        // Execute the GetElements operations
        for (final GetElements op : operations) {

            final Set<EntitySeed> opSeeds = difference(visitedSeeds, Sets.newHashSet(seeds));

            op.setInput(opSeeds);
            final Iterable<Edge> results = (Iterable<Edge>) store.execute(op, context);
            visitedSeeds.addAll(opSeeds);

            // Cache results
            Streams.toStream(results)
                    .forEach(e -> {
                        if (null != edgeTable.get(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue())) {
                            final Set<Edge> set = edgeTable.get(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue());
                            set.add(e);
                        } else {
                            final Set<Edge> set = new HashSet<>();
                            set.add(e);
                            edgeTable.put(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue(), set);
                        }
                    });

            final OperationChain<Set<? extends EntitySeed>> opChain = new OperationChain.Builder()
                    .first(new ToVertices.Builder()
                            .input(results)
                            .edgeVertices(ToVertices.EdgeVertices.DESTINATION)
                            .build())
                    .then(new ToEntitySeeds())
                    .then(new ToSet<>())
                    .build();

            seeds = store.execute(opChain, context);
        }

        // Track/recombine the edge objects and convert to return type
        return Streams.toStream(operation.getInput())
                .map(seed -> doPath(seed, edgeTable, new Stack<>()))
                .flatMap(List::stream)
                .map(path -> explodePath(path, edgeTable, new ArrayList<>()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private <T> List<Walk<Edge, Entity>> explodePath(final List<T> path, final HashBasedTable<?, ?, Set<Edge>> graph, final List<Edge> edges) {
        final List<Walk<Edge, Entity>> paths = new ArrayList<>();

        if (path.isEmpty()) {
            paths.add(edges);
        } else {
            for (int i = 1; i < path.size(); i++) {
                final List<Edge> edgeList = new ArrayList<>(graph.get(path.get(i - 1), path.get(i)));

                if (edgeList.size() == 1) {
                    edges.add(edgeList.get(0));
                } else {
                    for (final Edge edge : edgeList) {
                        final List<Edge> tmp = new ArrayList<>(edges);
                        tmp.add(edge);
                        paths.addAll(explodePath(path.subList(i - 1, i), graph, tmp));
                    }
                }
            }
            if (hops == edges.size()) {
                paths.add(edges);
            }
        }

        return paths;
    }

    private <T, U> List<Walk<Edge, Entity>> doPath(final T source, final HashBasedTable<T, T, ?> graph, final Stack<T> path) {
        final List<Walk<Void, Void>> walks = new ArrayList<>();

        path.push(source);

        if (pathVertices == path.size()) {
            walks.add(new Walk(path));
        } else {
            for (final T obj : graph.row(source).keySet()) {
                walks.addAll(new ArrayList<>(doPath(obj, graph, path)));
            }
        }

        path.pop();
        return walks;
    }

    private Set<EntitySeed> difference(final Set<Object> first, final Set<Object> second) {
        final Set<Object> difference = Sets.symmetricDifference(first, second);
        return difference.stream()
                .map(EntitySeed::new)
                .collect(Collectors.toSet());
    }
}
