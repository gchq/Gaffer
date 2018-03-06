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

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.graph.AdjacencyMap;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * An operation handler for {@link GetWalks} operations.
 * </p>
 * <p>
 * Currently the handler only supports creating {@link Walk}s which contain
 * {@link Edge}s.
 * </p>
 */
public class GetWalksHandler implements OutputOperationHandler<GetWalks, Iterable<Walk>> {

    private int hops;

    /**
     * Can be set by modifying operationsDeclarations.json.
     */
    private Integer maxHops = null;

    @Override
    public Iterable<Walk> doOperation(final GetWalks getWalks, final Context context, final Store store) throws OperationException {

        // Check input
        if (null == getWalks.getInput()) {
            return null;
        }

        // Check operations input
        if (null != getWalks.getOperations()) {
            hops = getWalks.getOperations().size();
        } else {
            return new EmptyClosableIterable<>();
        }

        // Check hops and maxHops (if set)
        if (hops == 0) {
            return new EmptyClosableIterable<>();
        } else if (maxHops != null && hops > maxHops) {
            throw new OperationException("GetWalks operation contains " + hops + " hops. The maximum number of hops is: " + maxHops);
        }

        final List<AdjacencyMap<Object, Edge>> adjacencyMaps = new ArrayList<>();

        List<Edge> results = null;

        // Execute the GetElements operations
        for (final GetElements getElements : getWalks.getOperations()) {
            results = executeGetElements(getElements, getWalks, context, store, results);

            // Store results in an AdjacencyMap
            final AdjacencyMap<Object, Edge> adjacencyMap = new AdjacencyMap<>();
            for (final Edge e : results) {
                adjacencyMap.put(e.getMatchedVertexValue(), e.getAdjacentMatchedVertexValue(), e);
            }

            adjacencyMaps.add(adjacencyMap);
        }

        // Track/recombine the edge objects and convert to return type
        return Streams.toStream(getWalks.getInput())
                .map(seed -> walk(seed.getVertex(), null, adjacencyMaps, new LinkedList<>()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public Integer getMaxHops() {
        return maxHops;
    }

    public void setMaxHops(final Integer maxHops) {
        this.maxHops = maxHops;
    }

    private List<Edge> executeGetElements(final GetElements getElements,
                                          final GetWalks getWalks,
                                          final Context context,
                                          final Store store,
                                          final List<Edge> results) throws OperationException {
        getElements.setView(new View.Builder()
                .merge(getElements.getView())
                .entities(Collections.emptyMap())
                .build());

        final OperationChain.OutputBuilder<CloseableIterable<? extends Element>> opChainBuilder;
        if (null == results) {
            getElements.setInput(getWalks.getInput());
            opChainBuilder = new OperationChain.Builder()
                    .first(getElements);
        } else {
            opChainBuilder = new OperationChain.Builder()
                    .first(new ToVertices.Builder()
                            .input(results)
                            .useMatchedVertex(ToVertices.UseMatchedVertex.OPPOSITE)
                            .build())
                    .then(new ToEntitySeeds())
                    .then(getElements);
        }

        // Limit the number of results if required
        final Output<? extends Iterable<? extends Element>> opChain;
        if (null != getWalks.getResultsLimit()) {
            opChain = opChainBuilder
                    .then(new Limit.Builder<Element>()
                            .resultLimit(getWalks.getResultsLimit())
                            .truncate(false)
                            .build())
                    .build();
        } else {
            opChain = opChainBuilder.build();
        }

        // Execute an the operation chain on the supplied store and cache
        // the results in memory using an ArrayList.
        return Lists.newArrayList((Iterable<Edge>) store.execute(opChain, context));
    }

    private List<Walk> walk(final Object curr, final Object prev, final List<AdjacencyMap<Object, Edge>> adjacencyMaps, final LinkedList<Set<Edge>> queue) {
        final List<Walk> walks = new ArrayList<>();

        if (null != prev && hops != queue.size()) { // if the walk is not at the maximum length
            queue.offer(adjacencyMaps.get(queue.size()).get(prev, curr));
        }

        if (hops == queue.size()) { // if the walk is at the maximum length
            final Walk.Builder builder = new Walk.Builder();
            for (final Set<Edge> edgeSet : queue) {
                builder.edges(edgeSet);
            }
            final Walk walk = builder.build();
            walks.add(walk);
        } else {
            for (final Object obj : adjacencyMaps.get(queue.size()).getDestinations(curr)) {
                walks.addAll(walk(obj, curr, adjacencyMaps, queue));
            }
        }

        if (!queue.isEmpty()) {
            queue.pollLast();
        }

        return walks;
    }
}
