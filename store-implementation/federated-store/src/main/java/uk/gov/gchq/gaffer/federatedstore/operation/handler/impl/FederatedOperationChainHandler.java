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

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler;

import java.util.List;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedOperationChainHandler<O> extends FederatedOperationOutputHandler<OperationChain<O>, O> {
    private final OperationChainHandler<O> defaultHandler;

    public FederatedOperationChainHandler(final OperationChainHandler<O> defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    @Override
    public O doOperation(final OperationChain<O> operationChain, final Context context, final Store store) throws OperationException {
        if (canHandleEntireChain(operationChain)) {
            ((OperationChainHandler) defaultHandler).prepareOperationChain(operationChain, context, store);
            return super.doOperation(operationChain, context, store);
        }

        return defaultHandler.doOperation(operationChain, context, store);
    }

    @Override
    protected O mergeResults(final List<O> results, final OperationChain<O> operationChain, final Context context, final Store store) {
        if (Void.class.equals(operationChain.getOutputClass())) {
            return null;
        }

        // Concatenate all the results into 1 iterable
        if (results.isEmpty()) {
            throw new IllegalArgumentException(NO_RESULTS_TO_MERGE_ERROR);
        }
        return (O) new ChainedIterable<>(results.toArray(new Iterable[results.size()]));
    }

    protected boolean canHandleEntireChain(final OperationChain<?> operationChain) {
        return canMergeResult(operationChain)
                && isHandledByTheSameGraphs(operationChain)
                && !FederatedOperation.hasFederatedOperations(operationChain);
    }

    /**
     * Checks whether the operation chain has an output type that we can easily
     * merge. This includes Iterables, CloseableIterables and Void outputs.
     *
     * @param operationChain the operation chain to check
     * @return true if we can merge the result easily.
     */
    private boolean canMergeResult(final OperationChain<?> operationChain) {
        final Class<?> outputClass = operationChain.getOutputClass();
        return outputClass.isAssignableFrom(CloseableIterable.class)
                || outputClass.isAssignableFrom(Iterable.class)
                || Void.class.equals(outputClass);
    }

    /**
     * Checks whether the operation chain is to be handled by the same graphs
     * or whether it needs to be split up and each operation executed on different
     * graphs.
     *
     * @param operationChain the operation chain to check
     * @return true if the operation chain is handled by the same graphs.
     */
    private boolean isHandledByTheSameGraphs(final OperationChain<?> operationChain) {
        return hasSameGraphIds(operationChain.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS), operationChain);
    }

    private boolean hasSameGraphIds(final String opChainGraphIds, final OperationChain<?> operationChain) {
        boolean hasSameIds = true;

        String graphIds = opChainGraphIds;
        for (final Operation operation : operationChain.getOperations()) {
            final String opGraphIds = operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS);
            if (null == graphIds) {
                graphIds = opGraphIds;
            } else if ((null != opGraphIds && !graphIds.equals(opGraphIds))
                    || (operation instanceof OperationChain && !hasSameGraphIds(graphIds, operationChain))) {
                hasSameIds = false;
                break;
            }
        }

        return hasSameIds;
    }
}
