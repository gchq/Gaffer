/*
 * Copyright 2021 Crown Copyright
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

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.nonNull;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.getSkipFailedFederatedStoreExecute;

/**
 * Abstract OP handler for the federation of an PAYLOAD operation with an expected return type OUTPUT.
 *
 * @param <OP>      The operation being handled
 * @param <OUTPUT>  The expected return type of the operation when handled.
 * @param <PAYLOAD> The operation to be federated and executed by delegate graphs.
 */
@Since("2.0.0")
public abstract class FederationHandler<OP extends Operation, OUTPUT, PAYLOAD extends Operation> implements OperationHandler<OP> {

    @Override
    public OUTPUT doOperation(final OP operation, final Context context, final Store store) throws OperationException {
        final List<OUTPUT> allGraphResults = getAllGraphResults(operation, context, (FederatedStore) store);
        KorypheBinaryOperator<OUTPUT> mergeFunction = getMergeFunction(operation);

        return mergeResults(allGraphResults, mergeFunction);

    }

    private List<OUTPUT> getAllGraphResults(final OP operation, final Context context, final FederatedStore store) throws OperationException {
        try {
            List<OUTPUT> results;
            final Collection<Graph> graphs = getGraphs(operation, context, store);
            results = new ArrayList<>(graphs.size());
            for (final Graph graph : graphs) {

//                PAYLOAD payloadOperation = getPayloadOperation(operation);
//                if (operation instanceof Input) {
//                    OperationHandlerUtil.updateOperationInput(payloadOperation, ((Input) operation).getInput());
//                }

                final Operation updatedOp = FederatedStoreUtil.updateOperationForGraph(getPayloadOperation(operation), graph);
                if (null != updatedOp) {
                    OUTPUT execute = null;
                    try {
                        if (updatedOp instanceof Output) {
                            //TODO x review this output distinction.
                            execute = graph.execute((Output<OUTPUT>) updatedOp, context);
                        } else {
                            graph.execute(updatedOp, context);
                        }
                    } catch (final Exception e) {
                        //TODO x make optional argument.
                        if (!Boolean.valueOf(getSkipFailedFederatedStoreExecute(updatedOp))) {
                            throw new OperationException(FederatedStoreUtil.createOperationErrorMsg(operation, graph.getGraphId(), e), e);
                        }
                    }
                    if (null != execute) {
                        results.add(execute);
                    }
                }
            }
            return results;
        } catch (final Exception e) {
            throw new OperationException("Error while running operation on graphs", e);
        }
    }

    abstract KorypheBinaryOperator<OUTPUT> getMergeFunction(OP operation);

    abstract PAYLOAD getPayloadOperation(final OP operation);

    abstract String getGraphIdsCsv(OP operation);

    protected OUTPUT mergeResults(final List<OUTPUT> results, final KorypheBinaryOperator<OUTPUT> mergeFunction) throws OperationException {
        try {
            OUTPUT rtn = null;
            if (nonNull(results)) {
                if (!isEmpty(results)) {
                    if (nonNull(mergeFunction)) {
                        for (final OUTPUT result : results) {
                            //TODO x Test voids & Nulls
                            rtn = mergeFunction.apply(rtn, result);
                        }
                    }
                } else {
                    //TODO X This is returning a OUTPUT not Iterable<OUTPUT> so returning a EmptyClosableIterable might be a mistake.
                    rtn = rtnDefaultWhenMergingNull();
                }
            }
            return rtn;
        } catch (final Exception e) {
            throw new OperationException("Error while merging results. " + e.getMessage(), e);
        }
    }

    protected OUTPUT rtnDefaultWhenMergingNull() {
        return null;
    }

    private Collection<Graph> getGraphs(final OP operation, final Context context, final FederatedStore store) {
        Collection<Graph> graphs = store.getGraphs(context.getUser(), getGraphIdsCsv(operation), operation);

        return nonNull(graphs) ?
                graphs
                : getDefaultGraphs(operation, context, store);
    }

    protected Collection<Graph> getDefaultGraphs(final OP operation, final Context context, final FederatedStore store) {
        return store.getDefaultGraphs(context.getUser(), operation);
    }
}
