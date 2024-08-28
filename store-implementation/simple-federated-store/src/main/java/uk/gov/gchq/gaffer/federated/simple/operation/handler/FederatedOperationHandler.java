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

package uk.gov.gchq.gaffer.federated.simple.operation.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * Main default handler for federated operations. Handles delegation to selected
 * graphs and will sub class the operation to a {@link FederatedOutputHandler}
 * if provided operation has output so that it is merged.
 */
public class FederatedOperationHandler<P extends Operation> implements OperationHandler<P> {

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
    public static final String OPT_AGGREGATE_ELEMENTS = "federated.aggregateElements";

    @Override
    public Object doOperation(P operation, Context context, Store store) throws OperationException {
        // If the operation has output wrap and return using sub class handler
        if (operation instanceof Output) {
            return new FederatedOutputHandler<>().doOperation((Output) operation, context, store);
        }

        List<GraphSerialisable> graphsToExecute = getGraphsToExecuteOn((FederatedStore) store, operation);

        if (graphsToExecute.isEmpty()) {
            return null;
        }

        // Execute the operation chain on each graph
        for (GraphSerialisable gs : graphsToExecute) {
            gs.getGraph().execute(operation, context.getUser());
        }

        // Assume no output, we've already checked above
        return null;
    }


    /**
     * Extract the graph IDs from the operation and process the option.
     * Will default to the store configured graph IDs if no option present.
     *
     * @param store The federated store.
     * @param operation The operation to execute.
     * @return List of {@link GraphSerialisable}s to execute on.
     */
    protected List<GraphSerialisable> getGraphsToExecuteOn(FederatedStore store, Operation operation) {
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
            graphsToExecute.add(store.getGraph(id));
        }

        return graphsToExecute;
    }

}
