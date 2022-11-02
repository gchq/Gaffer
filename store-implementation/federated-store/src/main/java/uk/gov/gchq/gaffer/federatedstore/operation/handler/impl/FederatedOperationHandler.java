/*
 * Copyright 2021-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.koryphe.Since;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getStoreConfiguredDefaultMergeFunction;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.processIfFunctionIsContextSpecific;

/**
 * FederatedOperation handler for the federation of an PAYLOAD operation with an expected return type OUTPUT
 */
@Since("2.0.0")
public class FederatedOperationHandler<INPUT, OUTPUT> implements OperationHandler<FederatedOperation<INPUT, OUTPUT>> {

    public static final String ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS_FORMAT = "Error while running operation on graphs, due to: %s";

    @Override
    public Object doOperation(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final Store store) throws OperationException {
        final Iterable<?> allGraphResults = getAllGraphResults(operation, context, (FederatedStore) store);

        return mergeResults(allGraphResults, operation, (FederatedStore) store, context);
    }

    private Iterable getAllGraphResults(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final FederatedStore store) throws OperationException {
        try {
            List<Object> results;
            final Collection<GraphSerialisable> graphs = getGraphs(operation, context, store);
            results = new ArrayList<>(graphs.size());
            for (final GraphSerialisable graphSerialisable : graphs) {
                final Graph graph = graphSerialisable.getGraph();

                final Operation updatedOp = FederatedStoreUtil.updateOperationForGraph(operation.getUnClonedPayload(), graph);
                if (updatedOp != null) {
                    try {
                        if (updatedOp instanceof Output) {
                            results.add(graph.execute((Output) updatedOp, context));
                        } else {
                            graph.execute(updatedOp, context);
                            if (nonNull(operation.getMergeFunction())) {
                                //If the user has specified a mergeFunction, they may wish to process the number null responses from graphs.
                                results.add(null);
                            }
                        }
                    } catch (final Exception e) {
                        if (!operation.isSkipFailedFederatedExecution()) {
                            throw new OperationException(FederatedStoreUtil.createOperationErrorMsg(operation, graphSerialisable.getGraphId(), e), e);
                        }
                    }
                }
            }

            return results;
        } catch (final Exception e) {
            throw new OperationException(String.format(ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS_FORMAT, e), e);
        }

    }

    private Object mergeResults(final Iterable resultsFromAllGraphs, final FederatedOperation operation, final FederatedStore store, final Context context) throws OperationException {
        try {
            Object rtn = null;

            final BiFunction mergeFunction = getMergeFunction(operation, store, context, isEmpty(resultsFromAllGraphs));

            //Reduce
            for (final Object resultFromAGraph : resultsFromAllGraphs) {
                rtn = mergeFunction.apply(resultFromAGraph, rtn);
            }

            return rtn;
        } catch (final Exception e) {
            throw new OperationException(String.format("Error while merging results. %s", Objects.toString(e.getMessage(), "")), e);
        }
    }

    private static BiFunction getMergeFunction(final FederatedOperation operation, final FederatedStore store, final Context context, final boolean isResultsFromAllGraphsEmpty) throws GafferCheckedException {
        final BiFunction mergeFunction;
        if (isResultsFromAllGraphsEmpty) {
            //No Merge function required.
            mergeFunction = null;
        } else if (nonNull(operation.getMergeFunction())) {
            //Get merge function from the Operation.
            final BiFunction operationMergeFunction = operation.getMergeFunction();
            //process if it is ContextSpecific
            mergeFunction = processIfFunctionIsContextSpecific(operationMergeFunction, operation.getPayloadOperation(), context, operation.getGraphIds(), store);
        } else {
            //Get merge function specified by the store.
            mergeFunction = getStoreConfiguredDefaultMergeFunction(operation.getPayloadOperation(), context, operation.getGraphIds(), store);
        }

        return mergeFunction;
    }

    private List<GraphSerialisable> getGraphs(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final FederatedStore store) {
        List<GraphSerialisable> graphs = store.getGraphs(context.getUser(), operation.getGraphIds(), operation);

        return nonNull(graphs) ?
                graphs
                : Collections.emptyList();
    }
}
