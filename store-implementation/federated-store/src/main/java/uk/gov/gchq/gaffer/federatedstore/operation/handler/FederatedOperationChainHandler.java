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

import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.lang.reflect.ParameterizedType;
import java.util.List;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedOperationChainHandler<O> extends FederatedOperationOutputHandler<OperationChain<O>, O> {
    private final OutputOperationHandler<? extends OperationChain<O>, O> defaultHandler;

    public FederatedOperationChainHandler(final OutputOperationHandler<? extends OperationChain<O>, O> defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    @Override
    public O doOperation(final OperationChain<O> operationChain, final Context context, final Store store) throws OperationException {
        final Class<?> outputClass = getOutputClass(operationChain);
        final boolean mergeableResultType = outputClass.isAssignableFrom(CloseableIterable.class)
                || outputClass.isAssignableFrom(Iterable.class)
                || Void.class.equals(outputClass);
        if (mergeableResultType
                && hasSameGraphIds(operationChain)
                && !FederatedOperation.hasFederatedOperations(operationChain)) {
            return super.doOperation(operationChain, context, store);
        }

        return ((OutputOperationHandler<OperationChain<O>, O>) defaultHandler).doOperation(operationChain, context, store);
    }

    @Override
    protected O mergeResults(final List<O> results, final OperationChain<O> operationChain, final Context context, final Store store) {
        if (Void.class.equals(getOutputClass(operationChain))) {
            return null;
        }

        // Concatenate all the results into 1 iterable
        if (results.isEmpty()) {
            throw new IllegalArgumentException(NO_RESULTS_TO_MERGE_ERROR);
        }
        return (O) new ChainedIterable<>(results.toArray(new Iterable[results.size()]));
    }

    private Class<?> getOutputClass(final OperationChain<O> operationChain) {
        final TypeReference<O> outputType = operationChain.getOutputTypeReference();
        return (Class) (outputType.getType() instanceof ParameterizedType ? ((ParameterizedType) outputType.getType()).getRawType() : outputType.getType());
    }

    private boolean hasSameGraphIds(final OperationChain<O> operationChain) {
        return hasSameGraphIds(operationChain.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS), operationChain);
    }

    private boolean hasSameGraphIds(final String opChainGraphIds, final OperationChain<O> operationChain) {
        String graphIds = opChainGraphIds;
        for (final Operation operation : operationChain.getOperations()) {
            final String opGraphIds = operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS);
            if (null == graphIds) {
                graphIds = opGraphIds;
            } else if (null != opGraphIds && !graphIds.equals(opGraphIds)) {
                return false;
            } else if (operation instanceof OperationChain && !hasSameGraphIds(graphIds, operationChain)) {
                return false;
            }
        }

        return true;
    }
}
