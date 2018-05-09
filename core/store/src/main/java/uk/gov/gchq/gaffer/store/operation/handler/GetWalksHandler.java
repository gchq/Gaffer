/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.UnwrapEntityId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.graph.GraphWindow;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.data.graph.adjacency.AdjacencyMap;
import uk.gov.gchq.gaffer.data.graph.adjacency.AdjacencyMaps;
import uk.gov.gchq.gaffer.data.graph.adjacency.PrunedAdjacencyMaps;
import uk.gov.gchq.gaffer.data.graph.adjacency.SimpleAdjacencyMaps;
import uk.gov.gchq.gaffer.data.graph.entity.EntityMap;
import uk.gov.gchq.gaffer.data.graph.entity.EntityMaps;
import uk.gov.gchq.gaffer.data.graph.entity.SimpleEntityMaps;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.koryphe.impl.function.IterableFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An operation handler for {@link GetWalks} operations.
 * <p>
 * The handler executes each {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements}
 * operation in the parent GetWalks operation in turn and incrementally creates
 * an in-memory representation of the resulting graph. Once all GetElements
 * operations have been executed, a recursive depth-first search algorithm is
 * used to construct all of the {@link Walk}s that exist in the temporary
 * graph.
 * <p>
 * The default handler has two settings which can be overridden by system
 * administrators: <ul> <li>maxHops - prevent users from executing GetWalks
 * operations that contain more than a set number of hops.</li> <li>prune -
 * toggle pruning for the in-memory graph representation. Enabling pruning
 * instructs the in-memory graph representation to discard any edges from the
 * previous GetElements operation which do not join up with any edges in the
 * current GetElements operation (orphaned edges). This reduces the memory
 * footprint of the in-memory graph representation, but requires some additional
 * processing while constructing the in-memory graph.</li> </ul>
 * <p>
 * The maxHops setting is not set by default (i.e. there is no limit to the
 * number of hops that a user can request). The prune flag is enabled by default
 * (for applications where performance is paramount and any issues arising from
 * excessive memory usage can be mitigated, this flag can be disabled).
 * <p>
 * This operation handler can be modified by supplying an
 * operationDeclarations.json file in order to limit the maximum number of hops
 * permitted or to enable/disable the pruning feature.
 * <p>
 * Currently the handler only supports creating {@link Walk}s which contain
 * {@link Edge}s.
 */
public class GetWalksHandler implements OutputOperationHandler<GetWalks, Iterable<Walk>> {
    private Integer maxHops = null;
    private boolean prune = true;

    @Override
    public Iterable<Walk> doOperation(final GetWalks getWalks, final Context context, final Store store) throws OperationException {
        // Check input
        if (null == getWalks.getInput()) {
            return null;
        }

        // Check there are some operations
        if (null == getWalks.getOperations()) {
            return new EmptyClosableIterable<>();
        }

        final Integer resultLimit = getWalks.getResultsLimit();
        final int hops = getWalks.getNumberOfGetEdgeOperations();


        final LimitedCloseableIterable limitedInputItr = new LimitedCloseableIterable<>(getWalks.getInput(), 0, resultLimit, false);
        final List<EntityId> originalInput = Lists.newArrayList(limitedInputItr);

        // Check hops and maxHops (if set)
        if (hops == 0) {
            return new EmptyClosableIterable<>();
        } else if (maxHops != null && hops > maxHops) {
            throw new OperationException("GetWalks operation contains " + hops + " hops. The maximum number of hops is: " + maxHops);
        }

        final AdjacencyMaps adjacencyMaps = prune ? new PrunedAdjacencyMaps() : new SimpleAdjacencyMaps();
        final EntityMaps entityMaps = new SimpleEntityMaps();

        List<?> seeds = originalInput;

        // Execute the operations
        for (final OperationChain<Iterable<Element>> operation : getWalks.getOperations()) {
            if (isWhileOperation(operation)) {
                seeds = executeWhileOperation(
                        operation, seeds, resultLimit,
                        context, store, hops, adjacencyMaps, entityMaps
                );
            } else {
                seeds = executeOperation(
                        operation, seeds, resultLimit,
                        context, store, hops, adjacencyMaps, entityMaps
                );
            }
        }

        // Must add an empty entity map at the end if one has not been explicitly
        // requested by the user.
        if (entityMaps.size() == adjacencyMaps.size()) {
            entityMaps.add(new EntityMap());
        }

        final GraphWindow graphWindow = new GraphWindow(adjacencyMaps, entityMaps);

        // Track/recombine the edge objects and convert to return type
        return Streams.toStream(originalInput)
                .flatMap(seed -> walk(seed.getVertex(), null, graphWindow, new LinkedList<>(), new LinkedList<>(), hops).stream())
                .collect(Collectors.toList());
    }

    public Integer getMaxHops() {
        return maxHops;
    }

    public void setMaxHops(final Integer maxHops) {
        this.maxHops = maxHops;
    }

    public Boolean getPrune() {
        return prune;
    }

    public void setPrune(final Boolean prune) {
        this.prune = prune;
    }

    private boolean isWhileOperation(final OperationChain<Iterable<Element>> operation) {
        return 1 == operation.getOperations().size()
                && operation.getOperations().get(0) instanceof While;
    }

    private List<?> executeWhileOperation(final OperationChain<Iterable<Element>> operation,
                                          final List<?> seeds,
                                          final Integer resultLimit,
                                          final Context context,
                                          final Store store,
                                          final int hops,
                                          final AdjacencyMaps adjacencyMaps,
                                          final EntityMaps entityMaps) throws OperationException {
        List<?> resultSeeds = seeds;
        final While whileOp = (While) operation.getOperations().get(0);
        if (null != whileOp.getOperation()) {
            validateWhileOperation(whileOp);
            final OperationHandler opHandler = store.getOperationHandler(While.class);
            if (null == opHandler) {
                throw new UnsupportedOperationException("While operations are not supported by this store");
            }
            if (!(opHandler instanceof WhileHandler)) {
                throw new UnsupportedOperationException("To use While operations within GetWalks, the While handler must be a " + WhileHandler.class.getName());
            }

            final WhileHandler whileHandler = (WhileHandler) opHandler;
            whileHandler.validateMaxRepeats(whileOp);

            // Change the transform to unwrap entity ids first.
            if (null != whileOp.getConditional() && null != whileOp.getConditional().getTransform()) {
                whileOp.getConditional().setTransform(new OperationChain<>(
                        new Map.Builder<>()
                                .first(new IterableFunction.Builder<>()
                                        .first(new UnwrapEntityId())
                                        .build())
                                .build(),
                        whileOp.getConditional().getTransform()));
            }

            for (int repeatCount = 0; repeatCount < whileOp.getMaxRepeats(); repeatCount++) {
                final While whileOpClone = whileOp.shallowClone();
                if (!whileHandler.isSatisfied(resultSeeds, whileOpClone, context, store)) {
                    break;
                }
                resultSeeds = executeOperation(
                        (Output) whileOpClone.getOperation(),
                        resultSeeds, resultLimit,
                        context, store, hops, adjacencyMaps, entityMaps
                );
            }
        }

        return resultSeeds;
    }

    private List<?> executeOperation(final Output<Iterable<Element>> operation,
                                     final List<?> seeds,
                                     final Integer resultLimit,
                                     final Context context,
                                     final Store store,
                                     final int hops,
                                     final AdjacencyMaps adjacencyMaps,
                                     final EntityMaps entityMaps) throws OperationException {
        final Iterable<Element> results = executeOperation(operation, seeds, resultLimit, context, store);

        final AdjacencyMap adjacencyMap = new AdjacencyMap();
        final EntityMap entityMap = new EntityMap();

        final List<Object> nextSeeds = new ArrayList<>();
        for (final Element e : results) {
            if (e instanceof Edge) {
                final Edge edge = (Edge) e;
                final Object nextSeed = edge.getAdjacentMatchedVertexValue();
                nextSeeds.add(nextSeed);
                adjacencyMap.putEdge(edge.getMatchedVertexValue(), nextSeed, edge);
            } else {
                final Entity entity = (Entity) e;
                entityMap.putEntity(entity.getVertex(), entity);
            }
        }

        if (hops > adjacencyMaps.size()) {
            adjacencyMaps.add(adjacencyMap);
        }
        entityMaps.add(entityMap);

        return nextSeeds;
    }

    private Iterable<Element> executeOperation(final Output<Iterable<Element>> operation,
                                               final List<?> seeds,
                                               final Integer resultLimit,
                                               final Context context,
                                               final Store store) throws OperationException {

        final Output<Iterable<Element>> convertedOp = new OperationChain.Builder()
                .first(new ToEntitySeeds.Builder()
                        .input(seeds)
                        .build())
                .then(OperationChain.wrap(operation))
                .build();

        // Execute an the operation chain on the supplied store and cache
        // the seeds in memory using an ArrayList.
        return new LimitedCloseableIterable<>(store.execute(convertedOp, context), 0, resultLimit, false);
    }

    private List<Walk> walk(final Object curr, final Object prev, final GraphWindow graphWindow, final LinkedList<Set<Edge>> edgeQueue, final LinkedList<Set<Entity>> entityQueue, final int hops) {
        final List<Walk> walks = new ArrayList<>();

        if (null != prev && hops != edgeQueue.size()) {
            edgeQueue.offer(graphWindow.getAdjacencyMaps().get(edgeQueue.size()).getEdges(prev, curr));
        }

        entityQueue.offer(graphWindow.getEntityMaps().get(entityQueue.size()).get(curr));

        if (hops == edgeQueue.size()) {
            final Walk walk = buildWalk(edgeQueue, entityQueue);
            walks.add(walk);
        } else {
            for (final Object obj : graphWindow.getAdjacencyMaps().get(edgeQueue.size()).getDestinations(curr)) {
                walks.addAll(walk(obj, curr, graphWindow, edgeQueue, entityQueue, hops));
            }
        }

        if (!edgeQueue.isEmpty()) {
            edgeQueue.pollLast();
        }

        if (!entityQueue.isEmpty()) {
            entityQueue.pollLast();
        }

        return walks;
    }

    private Walk buildWalk(final LinkedList<Set<Edge>> edgeQueue, final LinkedList<Set<Entity>> entityQueue) {
        final Walk.Builder builder = new Walk.Builder();

        final Iterator<Set<Edge>> edgeIterator = edgeQueue.iterator();
        final Iterator<Set<Entity>> entityIterator = entityQueue.iterator();

        while (edgeIterator.hasNext() || entityIterator.hasNext()) {
            if (entityIterator.hasNext()) {
                builder.entities(entityIterator.next());
            }
            if (edgeIterator.hasNext()) {
                builder.edges(edgeIterator.next());
            }
        }

        return builder.build();
    }

    private void validateWhileOperation(final While whileOp) {
        if (!(whileOp.getOperation() instanceof Output)
                || !Iterable.class.isAssignableFrom(((Output) whileOp.getOperation()).getOutputClass())) {
            throw new IllegalArgumentException(
                    "The While Operation delegate must be an operation that returns an Iterable of Elements. "
                            + whileOp.getOperation().getClass().getName() + " does not satisfy this."
            );
        }
    }
}
