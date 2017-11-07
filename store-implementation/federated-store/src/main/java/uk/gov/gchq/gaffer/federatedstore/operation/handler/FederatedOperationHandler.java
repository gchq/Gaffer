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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Collection;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.getSkipFailedFederatedStoreExecute;

/**
 * A handler for Operations with no output for FederatedStore
 *
 * @see OperationHandler
 * @see FederatedStore
 */
public class FederatedOperationHandler implements OperationHandler<Operation> {
    public Object doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        final Collection<Graph> graphs = ((FederatedStore) store).getGraphs(context.getUser(), operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS));
        for (final Graph graph : graphs) {
            final Operation updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);
            if (null != updatedOp) {
                try {
                    graph.execute(updatedOp, context);
                } catch (final Exception e) {
                    if (!Boolean.valueOf(getSkipFailedFederatedStoreExecute(updatedOp))) {
                        throw new OperationException(FederatedStoreUtil.createOperationErrorMsg(operation, graph.getGraphId(), e), e);
                    }
                }
            }
        }
        return null;
    }
}
