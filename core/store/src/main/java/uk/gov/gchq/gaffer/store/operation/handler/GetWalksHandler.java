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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.graph.AdjacencyList;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An operation handler for {@link GetWalks} operations.
 * <p>
 * Currently the handler only supports creating {@link Walk}s which contain
 * {@link Edge}s.
 */
public class GetWalksHandler implements OutputOperationHandler<GetWalks, Iterable<Walk>> {

    private int hops;

    @Override
    public Iterable<Walk> doOperation(final GetWalks getWalks, final Context context, final Store store) throws OperationException {

        // Check input
        if (null == getWalks.getInput()) {
            return null;
        }

        // Check operations input
        final Iterable<GetElements> operations = getWalks.getOperations();

        if (null == operations || Iterables.isEmpty(operations)) {
            return new EmptyClosableIterable<>();
        }

        hops = getWalks.getOperations().size();

        final List<AdjacencyList<Object, Edge>> adjacencyLists = new ArrayList<>();

        List<Edge> results = null;

        // Execute the GetElements operations
        for (final GetElements getElements : operations) {
            if (null == results) {
                getElements.setInput(getWalks.getInput());

                // Cache the results locally
                results = getLimitedResults(store, context, getWalks, getElements);
            } else {
                final OperationChain<CloseableIterable<? extends Element>> opChain = new OperationChain.Builder()
                        .first(new ToVertices.Builder()
                                .input(results)
                                .useMatchedVertex(ToVertices.UseMatchedVertex.OPPOSITE)
                                .build())
                        .then(new ToEntitySeeds())
                        .then(getElements)
                        .build();

                // Cache the results locally
                results = getLimitedResults(store, context, getWalks, opChain);
            }

            final AdjacencyList<Object, Edge> adjacencyList = new AdjacencyList<>();

            // Store results in an AdjacencyList
            for (final Edge e : results) {
                adjacencyList.put(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue(), e);
            }

            adjacencyLists.add(adjacencyList);
        }

        // Track/recombine the edge objects and convert to return type
        return Streams.toStream(getWalks.getInput())
                .map(seed -> walk(seed.getVertex(), null, adjacencyLists, new LinkedList<>()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<Walk> walk(final Object curr, final Object prev, final List<AdjacencyList<Object, Edge>> adjacencyLists, final LinkedList<Set<Edge>> queue) {
        final List<Walk> walks = new ArrayList<>();

        if (null != prev && hops != queue.size()) {
            queue.offer(adjacencyLists.get(queue.size()).get(prev, curr));
        }

        if (hops == queue.size()) {
            final Walk.Builder builder = new Walk.Builder();
            for (final Set<Edge> edgeSet : queue) {
                builder.edges(edgeSet);
            }
            final Walk walk = builder.build();
            walks.add(walk);
        } else {
            for (final Object obj : adjacencyLists.get(queue.size()).getDestinations(curr)) {
                walks.addAll(walk(obj, curr, adjacencyLists, queue));
            }
        }

        if (!queue.isEmpty()) {
            queue.pollLast();
        }

        return walks;
    }

    private <T> List<Edge> getLimitedResults(final Store store, final Context context, final GetWalks getWalks, final Output<T> opChain) throws OperationException {
        final Iterable<Edge> iterable = new LimitedCloseableIterable<Edge>((Iterable<Edge>) store.execute(opChain, context), 0, getWalks.getResultsLimit(), false);

        return Lists.newArrayList(iterable);
    }
}
