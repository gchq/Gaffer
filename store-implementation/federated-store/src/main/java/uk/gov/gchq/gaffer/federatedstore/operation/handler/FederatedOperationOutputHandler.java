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

package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A abstract handler for Operations with output for FederatedStore
 *
 * @see OperationHandler
 * @see FederatedStore
 * @see OutputOperationHandler
 */
public abstract class FederatedOperationOutputHandler<OP extends Output<O>, O> implements OutputOperationHandler<OP, O> {

    @Override
    public O doOperation(final OP operation, final Context context, final Store store) throws OperationException {
        final Collection<Graph> graphs = ((FederatedStore) store).getGraphs();
        final List<O> results = new ArrayList<>(graphs.size());
        for (final Graph graph : graphs) {
            final OP updatedOp = FederatedStore.updateOperationForGraph(operation, graph);
            if (null != updatedOp) {
                O execute = null;
                try {
                    execute = graph.execute(updatedOp, context.getUser());
                } catch (Exception e) {
                    if (!(updatedOp instanceof Options)
                            || !Boolean.valueOf(((Options) updatedOp).getOption(SKIP_FAILED_FEDERATED_STORE_EXECUTE))) {
                        throw new OperationException("Failed to execute " + operation.getClass().getSimpleName() + " on graph " + graph.getGraphId(), e);
                    }
                }
                if (execute != null) {
                    results.add(execute);
                }
            }
        }
        return mergeResults(results, operation, context, store);
    }

    protected abstract O mergeResults(final List<O> results, final OP operation, final Context context, final Store store) throws OperationException;
}
