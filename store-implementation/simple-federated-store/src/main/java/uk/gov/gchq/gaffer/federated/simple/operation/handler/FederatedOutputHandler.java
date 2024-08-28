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
import java.util.List;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.merge.DefaultResultAccumulator;
import uk.gov.gchq.gaffer.federated.simple.merge.FederatedResultAccumulator;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

/**
 * A sub class operation handler for federation that can process operations that have an
 * output associated with them. Will apply a {@link FederatedResultAccumulator} to merge
 * and reduce the results from multiple graphs into one.
 */
public class FederatedOutputHandler<P extends Output<O>, O>
        extends FederatedOperationHandler<P> implements OutputOperationHandler<P, O> {

    @Override
    public O doOperation(P operation, Context context, Store store) throws OperationException {
        List<GraphSerialisable> graphsToExecute = this.getGraphsToExecuteOn((FederatedStore) store, operation);

        if (graphsToExecute.isEmpty()) {
            return null;
        }

        // Execute the operation chain on each graph
        List<O> graphResults = new ArrayList<>();
        for (GraphSerialisable gs : graphsToExecute) {
            graphResults.add(gs.getGraph().execute(operation, context.getUser()));
        }

        // Not expecting any output so exit since we've executed
        if(operation.getOutputClass().isAssignableFrom(Void.class)) {
            return null;
        }

        // Set up the result accumulator
        FederatedResultAccumulator<O> resultAccumulator = new DefaultResultAccumulator<>();
        resultAccumulator.setSchema(((FederatedStore) store).getSchema(graphsToExecute));

        if (operation.containsOption(OPT_AGGREGATE_ELEMENTS)) {
            resultAccumulator.aggregateElements(Boolean.parseBoolean(operation.getOption(OPT_AGGREGATE_ELEMENTS)));
        }
        // Should now have a list of <O> objects so need to reduce to just one
        return graphResults.stream().reduce(resultAccumulator::apply).orElse(graphResults.get(0));
    }

}
