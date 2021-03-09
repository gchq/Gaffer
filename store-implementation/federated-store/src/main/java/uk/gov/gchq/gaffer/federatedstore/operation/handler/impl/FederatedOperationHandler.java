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

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.IterableConcat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.nonNull;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.getSkipFailedFederatedStoreExecute;

/**
 * FederatedOperation handler for the federation of an PAYLOAD operation with an expected return type OUTPUT
 */
@Since("2.0.0")
public class FederatedOperationHandler<INPUT, MIDPUT, OUTPUT> implements OperationHandler<FederatedOperation<INPUT, MIDPUT, OUTPUT>> {

    public static final String ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS = "Error while running operation on graphs";

    @Override
    public OUTPUT doOperation(final FederatedOperation<INPUT, MIDPUT, OUTPUT> operation, final Context context, final Store store) throws OperationException {
        final Iterable<MIDPUT> allGraphResults = getAllGraphResults(operation, context, (FederatedStore) store);
        Function<Iterable<MIDPUT>, OUTPUT> mergeFunction = operation.getMergeFunction();

        return mergeResults(allGraphResults, mergeFunction);

    }

    private ChainedIterable<MIDPUT> getAllGraphResults(final FederatedOperation<INPUT, MIDPUT, OUTPUT> operation, final Context context, final FederatedStore store) throws OperationException {
        try {
            List<MIDPUT> results;
            final Collection<Graph> graphs = getGraphs(operation, context, store);
            results = new ArrayList<>(graphs.size());
            for (final Graph graph : graphs) {

                //TODO FS Peer Review, Examine stitch the Input of FederatedOperation to PAYLOAD/updatedOp? Similar-1
//                PAYLOAD payloadOperation = getPayloadOperation(operation);
//                if (operation instanceof Input) {
//                    OperationHandlerUtil.updateOperationInput(payloadOperation, ((Input) operation).getInput());
//                }

                final Operation updatedOp = FederatedStoreUtil.updateOperationForGraph(operation.getPayloadOperation(), graph);
                if (null != updatedOp) {
                    try {
                        if (updatedOp instanceof Output) {
                            results.add(graph.execute((Output<MIDPUT>) updatedOp, context));
                        } else {
                            graph.execute(updatedOp, context);
                            //TODO FS Peer Review, return a null per graph?
                        }
                    } catch (final Exception e) {
                        //TODO FS Feature, make optional argument.
                        if (!Boolean.valueOf(getSkipFailedFederatedStoreExecute(updatedOp))) {
                            throw new OperationException(FederatedStoreUtil.createOperationErrorMsg(operation, graph.getGraphId(), e), e);
                        }
                    }
                }
            }

            return new ChainedIterable<>(results);
        } catch (
                final Exception e) {
            throw new OperationException(ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS, e);
        }

    }

    private OUTPUT mergeResults(final Iterable<MIDPUT> results, final Function<Iterable<MIDPUT>, OUTPUT> mergeFunction) throws OperationException {
        try {
            OUTPUT rtn;
            if (nonNull(mergeFunction)) {
                //TODO FS Test, voids & Nulls
                rtn = mergeFunction.apply(results);
            } else if (results.iterator().hasNext() && results.iterator().next() instanceof Iterable) {
                //Flatten
                //TODO FS USE COLLECTION CONCAT
                //TODO FS MAKE IT CONFIGURABLE
                Function<Iterable<MIDPUT>, OUTPUT> flattenFunction = (Iterable<MIDPUT> o) -> {
                    ArrayList assumedRtn = new ArrayList<>();
                    o.forEach(midput ->
                            {
                                if (nonNull(midput)) {
                                    ((Iterable) midput).forEach(assumedRtn::add);
                                }
                            }
                    );
                    return (OUTPUT) new ChainedIterable(assumedRtn);
                };
                rtn = flattenFunction.apply(results);
            } else {
                rtn = (OUTPUT) results;
            }
            return rtn;
        } catch (final Exception e) {
            throw new OperationException("Error while merging results. " + e.getMessage(), e);
        }
    }

    private Collection<Graph> getGraphs(final FederatedOperation<INPUT, MIDPUT, OUTPUT> operation, final Context context, final FederatedStore store) {
        Collection<Graph> graphs = store.getGraphs(context.getUser(), operation.getGraphIdsCSV(), operation);

        return nonNull(graphs) ?
                graphs
                //TODO FS Test Default
                : store.getDefaultGraphs(context.getUser(), operation);
    }

}
