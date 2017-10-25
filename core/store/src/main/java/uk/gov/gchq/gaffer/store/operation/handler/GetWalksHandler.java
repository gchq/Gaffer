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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.Walk;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An operation handler for {@link GetWalks} operations.
 *
 * Currently the handler only supports creating {@link Walk}s which contain {@link Edge}s.
 */
public class GetWalksHandler implements OutputOperationHandler<GetWalks, Iterable<Walk>> {

    private final Set<EntitySeed> visitedSeeds = new HashSet<>();
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

        Iterable<? extends EntitySeed> seeds = operation.getInput();

        hops = operation.getOperations().size();

        final HashBasedTable<Object, Object, Set<Edge>> subgraph = HashBasedTable.create();

        // Execute the GetElements operations
        for (final GetElements op : operations) {

            final Set<EntitySeed> opSeeds = difference(visitedSeeds, Sets.newHashSet(seeds));

            op.setInput(opSeeds);

            // Cache the results locally
            final List<Edge> results = Lists.newArrayList((Iterable<? extends Edge>) store.execute(op, context));

            visitedSeeds.addAll(opSeeds);

            // Cache results in HashBasedTable
            for (final Edge e : results) {
                if (null != subgraph.get(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue())) {
                    final Set<Edge> set = subgraph.get(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue());
                    set.add(e);
                } else {
                    final Set<Edge> set = new HashSet<>();
                    set.add(e);
                    subgraph.put(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue(), set);
                }
            }

            final OperationChain<Iterable<? extends EntitySeed>> opChain = new OperationChain.Builder()
                    .first(new ToVertices.Builder()
                            .input(results)
                            .edgeVertices(ToVertices.EdgeVertices.DESTINATION)
                            .build())
                    .then(new ToEntitySeeds())
                    .build();

            seeds = store.execute(opChain, context);
        }

        // Track/recombine the edge objects and convert to return type
        return Streams.toStream(operation.getInput())
                .map(seed -> walk(seed.getVertex(), null, subgraph, new LinkedList<>()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<Walk> walk(final Object curr, final Object prev, final HashBasedTable<Object, Object, Set<Edge>> subgraph, final LinkedList<Set<Edge>> queue) {
        final List<Walk> walks = new ArrayList<>();

        if (null != prev) {
            queue.offer(subgraph.get(prev, curr));
        }

        if (hops == queue.size()) {
            final Walk.Builder builder = new Walk.Builder();
            for (final Set<Edge> edgeSet : queue) {
                builder.edges(edgeSet);
            }
            final Walk walk = builder.build();
            walks.add(walk);
        } else {
            for (final Object obj : subgraph.row(curr).keySet()) {
                walks.addAll(walk(obj, curr, subgraph, queue));
            }
        }

        if (!queue.isEmpty()) {
            queue.pollLast();
        }

        return walks;
    }

    private Set<EntitySeed> difference(final Set<EntitySeed> first, final Set<EntitySeed> second) {
        final Set<EntitySeed> difference = Sets.symmetricDifference(first, second);
        return ImmutableSet.copyOf(difference);
    }
}
