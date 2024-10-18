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
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler for running federated operations but keeping the results separate
 * under a key of the graph ID the results come from.
 */
public class SeparateOutputHandler<P extends Output<O>, O> extends FederatedOperationHandler<P> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SeparateOutputHandler.class);

    @Override
    public Map<String, O> doOperation(final P operation, final Context context, final Store store) throws OperationException {
        List<GraphSerialisable> graphsToExecute = this.getGraphsToExecuteOn(operation, context, (FederatedStore) store);

        if (graphsToExecute.isEmpty()) {
            return new HashMap<>();
        }

        // Execute the operation chain on each graph
        LOGGER.debug("Returning separated graph results");
        Map<String, O> results = new HashMap<>();
        for (final GraphSerialisable gs : graphsToExecute) {
            try {
                results.put(gs.getGraphId(), gs.getGraph().execute(operation, context.getUser()));
            } catch (final OperationException | UnsupportedOperationException e) {
                // Optionally skip this error if user has specified to do so
                LOGGER.error("Operation failed on graph: {}", gs.getGraphId());
                if (!Boolean.parseBoolean(operation.getOption(OPT_SKIP_FAILED_EXECUTE, "false"))) {
                    throw e;
                }
            }
        }

        return results;
    }
}
