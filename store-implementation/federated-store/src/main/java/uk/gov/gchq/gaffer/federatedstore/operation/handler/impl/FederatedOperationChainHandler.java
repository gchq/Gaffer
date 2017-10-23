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
package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;

public class FederatedOperationChainHandler<O_ITEM> implements OutputOperationHandler<FederatedOperationChain<O_ITEM>, CloseableIterable<O_ITEM>> {
    @Override
    public CloseableIterable<O_ITEM> doOperation(final FederatedOperationChain<O_ITEM> operation, final Context context, final Store store) throws OperationException {
        final Collection<Graph> graphs = ((FederatedStore) store).getGraphs(context.getUser(), operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS));
        final List<Object> results = new ArrayList<>(graphs.size());
        for (final Graph graph : graphs) {
            final OperationChain updatedOp = FederatedStoreUtil.updateOperationForGraph(operation.getOperationChain(), graph);
            if (null != updatedOp) {
                Object result = null;
                try {
                    result = graph.execute(updatedOp, context.getUser());
                } catch (final Exception e) {
                    if (!Boolean.valueOf(updatedOp.getOption(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE))) {
                        throw new OperationException(FederatedStoreUtil.createOperationErrorMsg(operation, graph.getGraphId(), e), e);
                    }
                }
                if (null != result) {
                    results.add(result);
                }
            }
        }
        return mergeResults(results, operation, context, store);
    }

    protected CloseableIterable<O_ITEM> mergeResults(final List<Object> results, final FederatedOperationChain<O_ITEM> operation, final Context context, final Store store) {
        if (Void.class.equals(operation.getOperationChain().getOutputClass())) {
            return null;
        }

        if (results.isEmpty()) {
            return new WrappedCloseableIterable<>(Collections.emptyList());
        }

        boolean areIterable = true;
        for (final Object result : results) {
            if (!(result instanceof Iterable)) {
                areIterable = false;
                break;
            }
        }

        if (areIterable) {
            return new ChainedIterable<>(CollectionUtil.toIterableArray((List) results));
        }

        return new WrappedCloseableIterable(results);
    }
}
