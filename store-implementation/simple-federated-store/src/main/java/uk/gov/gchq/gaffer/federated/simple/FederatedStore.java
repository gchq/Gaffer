package uk.gov.gchq.gaffer.federated.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.DefaultFederatedHandler;
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
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class FederatedStore extends Store {

    // Default graph IDs to execute on
    private List<String> defaultGraphIds = new ArrayList<>();

    // Cached list of available graphs
    private List<GraphSerialisable> graphs = new ArrayList<>();

    /**
     * Add a new graph so that it is available to this federated store.
     *
     * @param graph The serialisable instance of the graph.
     */
    public void addGraph(GraphSerialisable graph) {
        graphs.add(graph);
    }

    /**
     * Get the {@link GraphSerialisable} from a given graph ID. The graph
     * must obviously be known to this federated store to be returned.
     *
     * @param graphId The graph ID
     * @return The {@link GraphSerialisable} relating to the ID.
     *
     * @throws GafferCheckedException If graph not found.
     */
    public GraphSerialisable getGraph(String graphId) throws GafferCheckedException {
        for (GraphSerialisable graph : graphs) {
            if (graph.getGraphId().equals(graphId)) {
                return graph;
            }
        }

        throw new GafferCheckedException("Graph for Graph ID: " + graphId + " not available to federated store");
    }

    /**
     * Get the default list of graph IDs for this federated store.
     *
     * @return The default list.
     */
    public List<String> getDefaultGraphIds() {
        return defaultGraphIds;
    }

    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, new Schema(), properties);
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        try {
            // Use the default federated handler so operations are forwarded to graphs
            return new DefaultFederatedHandler<>().doOperation((OperationChain<Object>) OperationChain.wrap(operation), context, this);
        } catch (OperationException e) {
            throw new GafferRuntimeException(e.getMessage());
        }
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        ///
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        return null;
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }

}
