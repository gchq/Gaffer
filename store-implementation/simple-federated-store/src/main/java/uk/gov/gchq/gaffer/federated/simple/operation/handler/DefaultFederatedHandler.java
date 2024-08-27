package uk.gov.gchq.gaffer.federated.simple.operation.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.merge.DefaultResultAccumulator;
import uk.gov.gchq.gaffer.federated.simple.merge.FederatedResultAccumulator;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class DefaultFederatedHandler<O> implements OperationHandler<OperationChain<O>> {

    /**
     * The operation option for the Graph IDs that an operation should be
     * executed on, will take preference over the short variant of this option.
     */
    public static final String OPT_GRAPH_IDS = "gaffer.federatedstore.operation.graphIds";

    /**
     * The short version of the operation option for the Graph IDs that an
     * operation should be executed on.
     */
    public static final String OPT_SHORT_GRAPH_IDS = "federated.graphIds";

    /**
     * The boolean operation option to specify if element merging should be applied or not.
     */
    public static final String OPT_MERGE_ELEMENTS = "federated.mergeElements";

    @Override
    public Object doOperation(OperationChain<O> operation, Context context, Store store) throws OperationException {
        List<GraphSerialisable> graphsToExecute = getGraphsToExecuteOn((FederatedStore) store, operation);

        if (graphsToExecute.isEmpty()) {
            return Collections.emptySet();
        }

        // Execute the operation chain on each graph
        List<O> graphResults = new ArrayList<>();
        for (GraphSerialisable gs : graphsToExecute) {
            graphResults.add(gs.getGraph().execute(operation, context.getUser()));
        }

        // Not expecting any output so exit
        if(operation.getOutputClass().isAssignableFrom(Void.class)) {
            return null;
        }

        // Set up the result accumulator
        FederatedResultAccumulator<O> resultAccumulator = new DefaultResultAccumulator<>();
        if (operation.containsOption(OPT_MERGE_ELEMENTS)) {
            resultAccumulator.setMergeElements(Boolean.parseBoolean(operation.getOption(OPT_MERGE_ELEMENTS)));
        }
        // Should now have a list of <O> objects so need to reduce to just one
        return graphResults.stream().reduce(resultAccumulator::apply).orElse(graphResults.get(0));
    }


    /**
     * Extract the graph IDs from the operation and process the option.
     * Will default to the store configured graph IDs if no option present.
     *
     * @param store The federated store.
     * @param operation The operation to execute.
     * @return List of {@link GraphSerialisable}s to execute on.
     *
     * @throws OperationException If issue getting graphs.
     */
    private List<GraphSerialisable> getGraphsToExecuteOn(FederatedStore store, Operation operation) throws OperationException {
        List<String> graphIds = store.getDefaultGraphIds();
        List<GraphSerialisable> graphsToExecute = new ArrayList<>();
        // If user specified graph IDs for this chain parse as comma separated list
        if (operation.containsOption(OPT_GRAPH_IDS)) {
            graphIds = Arrays.asList(operation.getOption(OPT_GRAPH_IDS).split(","));
        } else if (operation.containsOption(OPT_SHORT_GRAPH_IDS)) {
            graphIds = Arrays.asList(operation.getOption(OPT_SHORT_GRAPH_IDS).split(","));
        }

        // Get the corresponding graph serialisable
        for (String id : graphIds) {
            try {
                graphsToExecute.add(store.getGraph(id));
            } catch (GafferCheckedException e) {
                throw new OperationException("Failed to run operation on Graph with ID: " + id, e);
            }
        }

        return graphsToExecute;
    }

}
