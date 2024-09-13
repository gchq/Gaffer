/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federated.simple.operation.AddGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federated.simple.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOutputHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.add.AddGraphHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.get.GetAllGraphIdsHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.get.GetSchemaHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.misc.RemoveGraphHandler;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The federated store implementation. Provides the set up and required
 * methods to enable a {@link Store} that will delegate {@link Operation}s
 * to sub graphs then merge the result.
 */
public class FederatedStore extends Store {

    // Default graph IDs to execute on
    private List<String> defaultGraphIds = new LinkedList<>();

    // Cached list of available graphs
    private final List<GraphSerialisable> graphs = new LinkedList<>();

    // Store specific handlers
    public final Map<Class<? extends Operation>, OperationHandler<?>> storeHandlers = Stream.of(
            new SimpleEntry<>(AddGraph.class, new AddGraphHandler()),
            new SimpleEntry<>(GetAllGraphIds.class, new GetAllGraphIdsHandler()),
            new SimpleEntry<>(GetSchema.class, new GetSchemaHandler()),
            new SimpleEntry<>(RemoveGraph.class, new RemoveGraphHandler()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    /**
     * Add a new graph so that it is available to this federated store.
     *
     * @param graph The serialisable instance of the graph.
     *
     * @throws IllegalArgumentException If there is already a graph with the supplied ID
     */
    public void addGraph(final GraphSerialisable graph) {
        // Make sure graph ID doesn't already exist
        if (graphs.stream().map(GraphSerialisable::getGraphId).anyMatch(id -> id.equals(graph.getGraphId()))) {
            throw new IllegalArgumentException(
                "A graph with Graph ID: '" + graph.getGraphId() + "' has already been added to this store");
        }
        graphs.add(new GraphSerialisable(graph.getConfig(), graph.getSchema(), graph.getStoreProperties()));
    }

    /**
     * Remove a graph from the scope of this store.
     *
     * @param graphId The graph ID of the graph to remove.
     *
     * @throws IllegalArgumentException If graph not found.
     */
    public void removeGraph(final String graphId) {
        GraphSerialisable graphToRemove = getGraph(graphId);
        graphs.remove(graphToRemove);
    }

    /**
     * Get the {@link GraphSerialisable} from a given graph ID. The graph
     * must obviously be known to this federated store to be returned.
     *
     * @param graphId The graph ID
     * @return The {@link GraphSerialisable} relating to the ID.
     *
     * @throws IllegalArgumentException If graph not found.
     */
    public GraphSerialisable getGraph(final String graphId) {
        for (final GraphSerialisable graph : graphs) {
            if (graph.getGraphId().equals(graphId)) {
                return graph;
            }
        }
        throw new IllegalArgumentException("Graph with Graph ID: '" + graphId + "' is not available to this federated store");
    }

    /**
     * Returns a list of all the graphs available to this store.
     *
     * @return List of {@link GraphSerialisable}s
     */
    public List<GraphSerialisable> getAllGraphs() {
        return graphs;
    }

    /**
     * Get the default list of graph IDs for this federated store.
     *
     * @return The default list.
     */
    public List<String> getDefaultGraphIds() {
        return defaultGraphIds;
    }

    /**
     * Set the default list of graph IDs for this federated store.
     *
     * @param defaultGraphIds Default list to set.
     */
    public void setDefaultGraphIds(final List<String> defaultGraphIds) {
        this.defaultGraphIds = defaultGraphIds;
    }

    /**
     * Gets a merged schema based on the graphs specified.
     *
     * @param graphs The graphs to get the schemas from.
     * @return A merged {@link Schema}
     */
    public Schema getSchema(final List<GraphSerialisable> graphs) {
        try {
            // Get the graph IDs we care about
            List<String> graphIds = new ArrayList<>();
            graphs.forEach(gs -> graphIds.add(gs.getGraphId()));

            // Create and run a get schema operation with relevant IDs
            GetSchema operation = new GetSchema.Builder()
                    .option(FederatedOperationHandler.OPT_SHORT_GRAPH_IDS, String.join(",", graphIds))
                    .build();
            return (Schema) this.handleOperation(operation, new Context());

        } catch (final OperationException e) {
            throw new GafferRuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Access to getting the operations that have handlers specific to this
     * store.
     *
     * @return The Operation classes handled by this store.
     */
    public Set<Class<? extends Operation>> getStoreSpecificOperations() {
        return storeHandlers.keySet();
    }

    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        if (unused != null) {
            throw new IllegalArgumentException("Federated store should not be initialised with a Schema");
        }
        super.initialise(graphId, new Schema(), properties);
    }

    @Override
    public Schema getSchema() {
        // Return a blank schema if we have no default graphs
        if (defaultGraphIds.isEmpty()) {
            return new Schema();
        }

        try {
            return (Schema) this.handleOperation(new GetSchema(), new Context());
        } catch (final OperationException e) {
            throw new GafferRuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        try {
            // Use a federated handler so operations are forwarded to graphs
            return new FederatedOperationHandler<>().doOperation(operation, context, this);
        } catch (final OperationException e) {
            throw new GafferRuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        storeHandlers.forEach(this::addOperationHandler);
    }

    @Override
    protected OperationHandler<? extends OperationChain<?>> getOperationChainHandler() {
        return new FederatedOperationHandler<>();
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new FederatedOperationHandler<>();
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        return new FederatedOperationHandler<>();
    }

    @Override
    protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
        return new FederatedOperationHandler<>();
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }
}
