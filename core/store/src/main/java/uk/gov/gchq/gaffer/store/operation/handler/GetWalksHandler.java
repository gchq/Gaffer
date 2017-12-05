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

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.graph.GraphWindow;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.data.graph.adjacency.AdjacencyMap;
import uk.gov.gchq.gaffer.data.graph.adjacency.AdjacencyMaps;
import uk.gov.gchq.gaffer.data.graph.adjacency.PrunedAdjacencyMaps;
import uk.gov.gchq.gaffer.data.graph.adjacency.SimpleAdjacencyMaps;
import uk.gov.gchq.gaffer.data.graph.entity.EntityMap;
import uk.gov.gchq.gaffer.data.graph.entity.EntityMaps;
import uk.gov.gchq.gaffer.data.graph.entity.SimpleEntityMaps;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.WalkDefinition;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

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

    private long hops;

    private Integer maxHops = null;

    private boolean prune = true;

    @Override
    public Iterable<Walk> doOperation(final GetWalks getWalks, final Context context, final Store store) throws OperationException {

        // Check input
        if (null == getWalks.getInput()) {
            return null;
        }

        // Check walk definitions input
        if (null != getWalks.getWalkDefinitions()) {

            hops = getWalks.getWalkDefinitions().stream()
                    .filter(wd -> wd.getOperation().getView().hasEdges())
                    .count();
        } else {
            return new EmptyClosableIterable<>();
        }

        // Check hops and maxHops (if set)
        if (hops == 0) {
            return new EmptyClosableIterable<>();
        } else if (maxHops != null && hops > maxHops) {
            throw new OperationException("GetWalks operation contains " + hops + " hops. The maximum number of hops is: " + maxHops);
        }

        final AdjacencyMaps adjacencyMaps = prune ? new PrunedAdjacencyMaps() : new SimpleAdjacencyMaps();
        final EntityMaps entityMaps = new SimpleEntityMaps();

        List<Element> results = null;

        // Execute the walk definitions
        for (final WalkDefinition walkDefinition : getWalks.getWalkDefinitions()) {
            results = executeWalkDefinition(walkDefinition, getWalks, context, store, results);

            final AdjacencyMap adjacencyMap = new AdjacencyMap();
            final EntityMap entityMap = new EntityMap();

            boolean edges = false;
            boolean entities = false;

            for (final Element e : results) {

                if (e instanceof Edge) {
                    final Edge edge = (Edge) e;
                    adjacencyMap.putEdge(edge.getMatchedVertexValue(), edge.getAdjacentMatchedVertexValue(), edge);
                    edges = true;
                } else {
                    final Entity entity = (Entity) e;
                    entityMap.putEntity(entity.getVertex(), entity);
                    entities = true;
                }
            }

            if (edges) {
                if (entityMaps.empty() || (entityMaps.size() == adjacencyMaps.size())) {
                    entityMaps.add(new EntityMap());
                }

                adjacencyMaps.add(adjacencyMap);
            }

            if (entities) {
                entityMaps.add(entityMap);
            }
        }

        // Must add an empty entity map at the end if one has not been explicitly
        // requested by the user.
        if (entityMaps.size() == adjacencyMaps.size()) {
            entityMaps.add(new EntityMap());
        }

        final GraphWindow graphWindow = new GraphWindow(adjacencyMaps, entityMaps);

        // Track/recombine the edge objects and convert to return type
        return Streams.toStream(getWalks.getInput())
                .flatMap(seed -> walk(seed.getVertex(), null, graphWindow, new LinkedList<>(), new LinkedList<>()).stream())
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

    private List<Element> executeWalkDefinition(final WalkDefinition walkDef,
                                                final GetWalks getWalks,
                                                final Context context,
                                                final Store store,
                                                final List<Element> results) throws OperationException {

        final OperationChain.OutputBuilder<? extends Iterable<? extends ElementId>> opChainBuilder;

        if (null == results) {
            walkDef.getOperation().setInput(getWalks.getInput());
            if (!walkDef.getPreFilters().getOperations().isEmpty()) {
                ((Input) walkDef.getPreFilters().getOperations().get(0)).setInput(getWalks.getInput());
                opChainBuilder = new OperationChain.Builder()
                        .first(walkDef.getPreFilters())
                        .then(walkDef.getOperation());
            } else {
                opChainBuilder = new OperationChain.Builder()
                        .first(walkDef.getOperation());
            }
        } else {

            opChainBuilder = new OperationChain.Builder()
                    .first(new ToVertices.Builder()
                            .input(results)
                            .useMatchedVertex(ToVertices.UseMatchedVertex.OPPOSITE)
                            .build())
                    .then(new ToEntitySeeds());

            if (null != walkDef.getPreFilters()) {
                for (final Operation op : walkDef.getPreFilters().getOperations()) {
                    opChainBuilder.then((Input) op);
                }
            }

            opChainBuilder.then(walkDef.getOperation());
        }

        if (!walkDef.getPostFilters().getOperations().isEmpty()) {
            for (final Operation op : walkDef.getPostFilters().getOperations()) {
                opChainBuilder.then((Input) op);
            }
        }

        // Execute an the operation chain on the supplied store and cache
        // the results in memory using an ArrayList.
        return Lists.newArrayList((Iterable<Edge>) store.execute(opChainBuilder.build(), context));
    }

    private List<Walk> walk(final Object curr, final Object prev, final GraphWindow graphWindow, final LinkedList<Set<Edge>> edgeQueue, final LinkedList<Set<Entity>> entityQueue) {
        final List<Walk> walks = new ArrayList<>();

        if (null != prev && hops != edgeQueue.size()) {
            edgeQueue.offer(graphWindow.getAdjacencyMaps().get(edgeQueue.size()).getEdges(prev, curr));
        }

        entityQueue.offer(graphWindow.getEntityMaps().get(entityQueue.size()).get(curr));

        if (hops == edgeQueue.size()) {
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

            final Walk walk = builder.build();
            walks.add(walk);
        } else {
            for (final Object obj : graphWindow.getAdjacencyMaps().get(edgeQueue.size()).getDestinations(curr)) {
                walks.addAll(walk(obj, curr, graphWindow, edgeQueue, entityQueue));
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
}
