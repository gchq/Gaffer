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
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.getSkipFailedFederatedStoreExecute;

/**
 * A abstract handler for Operations with output for FederatedStore
 *
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler
 */
public abstract class FederatedOperationOutputHandler<OP extends Output<O>, O> implements OutputOperationHandler<OP, O> {
    public static final String NO_RESULTS_TO_MERGE_ERROR = "The federated operation received no results to merge. A common cause to this is that the operation was performed against no graphs due to visibility and access.";

    @Override
    public O doOperation(final OP operation, final Context context, final Store store) throws OperationException {
        final Collection<Graph> graphs = ((FederatedStore) store).getGraphs(context.getUser(), operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS));
        final List<O> results = new ArrayList<>(graphs.size());
        for (final Graph graph : graphs) {
            final OP updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);
            if (null != updatedOp) {
                O execute = null;
                try {
                    execute = graph.execute(updatedOp, context.getUser());
                } catch (final Exception e) {
                    if (!Boolean.valueOf(getSkipFailedFederatedStoreExecute(updatedOp))) {
                        throw new OperationException(FederatedStoreUtil.createOperationErrorMsg(operation, graph.getGraphId(), e), e);
                    }
                }
                if (null != execute) {
                    results.add(execute);
                }
            }
        }
        try {
            return mergeResults(results, operation, context, store);
        } catch (final Exception e) {
            throw new OperationException(e);
        }
    }

    protected abstract O mergeResults(final List<O> results, final OP operation, final Context context, final Store store);
}
