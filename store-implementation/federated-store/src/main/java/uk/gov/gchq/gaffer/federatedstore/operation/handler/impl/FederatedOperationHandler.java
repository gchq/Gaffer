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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * FederatedOperation handler for the federation of an PAYLOAD operation with an expected return type OUTPUT
 */
@Since("2.0.0")
public class FederatedOperationHandler<INPUT, OUTPUT> implements OperationHandler<FederatedOperation<INPUT, OUTPUT>> {

    public static final String ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS = "Error while running operation on graphs";

    @Override
    public OUTPUT doOperation(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final Store store) throws OperationException {
        final Iterable<?> allGraphResults = getAllGraphResults(operation, context, (FederatedStore) store);
        Function<Iterable<?>, OUTPUT> mergeFunction = operation.getMergeFunction();

        return mergeResults(allGraphResults, mergeFunction, (FederatedStore) store);
    }

    private Iterable getAllGraphResults(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final FederatedStore store) throws OperationException {
        try {
            List<Object> results;
            final Collection<Graph> graphs = getGraphs(operation, context, store);
            results = new ArrayList<>(graphs.size());
            for (final Graph graph : graphs) {

                final Operation updatedOp = FederatedStoreUtil.updateOperationForGraph(operation.getUnClonedPayload(), graph);
                if (null != updatedOp) {
                    try {
                        if (updatedOp instanceof Output) {
                            results.add(graph.execute((Output) updatedOp, context));
                        } else {
                            graph.execute(updatedOp, context);
                            if (nonNull(operation.getMergeFunction())) {
                                results.add(null);
                            }
                        }
                    } catch (final Exception e) {
                        if (!operation.isSkipFailedFederatedExecution()) {
                            throw new OperationException(FederatedStoreUtil.createOperationErrorMsg(operation, graph.getGraphId(), e), e);
                        }
                    }
                }
            }

            return results;
        } catch (
                final Exception e) {
            throw new OperationException(ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS, e);
        }

    }

    private OUTPUT mergeResults(final Iterable results, final Function<Iterable<?>, OUTPUT> mergeFunction, final FederatedStore store) throws OperationException {
        try {
            OUTPUT rtn;
            if (nonNull(mergeFunction)) {
                rtn = mergeFunction.apply(results);
            } else if (results.iterator().hasNext() && results.iterator().next() instanceof Iterable) {
                rtn = (OUTPUT) store.getDefaultMergeFunction().apply(results);
            } else {
                rtn = (OUTPUT) results;
            }
            return rtn;
        } catch (final Exception e) {
            String message = e.getMessage();
            throw new OperationException(String.format("Error while merging results. %s", isNull(message) ? "" : message), e);
        }
    }

    private Collection<Graph> getGraphs(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final FederatedStore store) {
        Collection<Graph> graphs = store.getGraphs(context.getUser(), operation.getGraphIdsCSV(), operation);

        return nonNull(graphs) ?
                graphs
                //TODO FS Test Default
                : store.getDefaultGraphs(context.getUser(), operation);
    }

}
