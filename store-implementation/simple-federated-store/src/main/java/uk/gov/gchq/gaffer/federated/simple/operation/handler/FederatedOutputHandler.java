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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.merge.DefaultResultAccumulator;
import uk.gov.gchq.gaffer.federated.simple.merge.FederatedResultAccumulator;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A sub class operation handler for federation that can process operations that have an
 * output associated with them. Will apply a {@link FederatedResultAccumulator} to merge
 * and reduce the results from multiple graphs into one.
 */
public class FederatedOutputHandler<P extends Output<O>, O>
        extends FederatedOperationHandler<P> implements OutputOperationHandler<P, O> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedOutputHandler.class);

    @Override
    public O doOperation(final P operation, final Context context, final Store store) throws OperationException {
        List<GraphSerialisable> graphsToExecute = this.getGraphsToExecuteOn(operation, context, (FederatedStore) store);

        if (graphsToExecute.isEmpty()) {
            return null;
        }

        // Execute the operation chain on each graph
        List<O> graphResults = new ArrayList<>();
        for (final GraphSerialisable gs : graphsToExecute) {
            try {
                graphResults.add(gs.getGraph().execute(operation, context.getUser()));
            } catch (final OperationException | UnsupportedOperationException e) {
                // Optionally skip this error if user has specified to do so
                LOGGER.error("Operation failed on graph: {}", gs.getGraphId());
                if (!Boolean.parseBoolean(operation.getOption(OPT_SKIP_FAILED_EXECUTE, "false"))) {
                    throw e;
                }
                LOGGER.info("Continuing operation execution on sub graphs");
            } catch (final IllegalArgumentException e) {
                // An operation may fail validation for a sub graph this is not really an error.
                // We can just continue to execute on the rest of the graphs
                LOGGER.warn("Operation contained invalid arguments for a sub graph, skipped execution on graph: {}", gs.getGraphId());
            }
        }

        // Not expecting any output so exit since we've executed
        if (operation.getOutputClass() == Void.class || graphResults.isEmpty()) {
            return null;
        }

        // Merge the store props with the operation options for setting up the accumulator
        Properties combinedProps = store.getProperties().getProperties();
        if (operation.getOptions() != null) {
            combinedProps.putAll(operation.getOptions());
        }

        // Set up the result accumulator
        FederatedResultAccumulator<O> resultAccumulator = new DefaultResultAccumulator<>(combinedProps);
        resultAccumulator.setSchema(((FederatedStore) store).getSchema(graphsToExecute));

        if (operation.containsOption(OPT_AGGREGATE_ELEMENTS)) {
            resultAccumulator.setAggregateElements(Boolean.parseBoolean(operation.getOption(OPT_AGGREGATE_ELEMENTS)));
        }
        // Should now have a list of <O> objects so need to reduce to just one
        return graphResults.stream().reduce(resultAccumulator::apply).orElse(graphResults.get(0));
    }

}
