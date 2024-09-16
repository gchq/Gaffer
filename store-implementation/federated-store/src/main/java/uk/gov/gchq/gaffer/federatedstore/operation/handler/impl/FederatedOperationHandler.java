/*
 * Copyright 2021-2023 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.ApplyViewToElementsFunction;
import uk.gov.gchq.gaffer.federatedstore.util.ConcatenateMergeFunction;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.Since;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getStoreConfiguredMergeFunction;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.processIfFunctionIsContextSpecific;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.updateOperationForGraph;

/**
 * FederatedOperation handler for the federation of an PAYLOAD operation with an expected return type OUTPUT
 */
@Since("2.0.0")
public class FederatedOperationHandler<INPUT, OUTPUT> implements OperationHandler<FederatedOperation<INPUT, OUTPUT>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedOperationHandler.class);

    public static final String ERROR_WHILE_RUNNING_OPERATION_ON_GRAPHS_FORMAT = "Error while running operation on graphs, due to: %s";
    private List<GraphSerialisable> graphs;
    private Context context;

    @Override
    public Object doOperation(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final Store store) throws OperationException {
        this.context = context;
        this.graphs = getGraphs(operation, context, (FederatedStore) store);
        final Iterable<?> allGraphResults = getAllGraphResults(operation);

        return mergeResults(allGraphResults, operation, (FederatedStore) store);
    }

    private Iterable getAllGraphResults(final FederatedOperation<INPUT, OUTPUT> operation) throws OperationException {
        try {
            List<Object> results;
            results = new ArrayList<>(graphs.size());
            LOGGER.debug("Getting results from {} graphs", graphs.size());
            for (final GraphSerialisable graphSerialisable : graphs) {
                final Graph graph = graphSerialisable.getGraph();

                final Operation updatedOp = updateOperationForGraph(operation.getUnClonedPayload(), graph, context);
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

    private Object mergeResults(final Iterable resultsFromAllGraphs, final FederatedOperation<INPUT, OUTPUT> operation, final FederatedStore store) throws OperationException {
        try {
            Object rtn = null;

            BiFunction mergeFunction = getMergeFunction(operation, store, context, isEmpty(resultsFromAllGraphs));

            // If default merging and only have one graph or no common groups then just return the current results
            if (!graphs.isEmpty()
                    && mergeFunction instanceof ApplyViewToElementsFunction
                    && (graphs.size() == 1 || !graphsHaveCommonSchemaGroups(graphs))) {
                LOGGER.info("Returning flat list of results as complex merging not required when only one graph or no common groups");
                // Just use the concatenate merge to flatten the results
                mergeFunction = new ConcatenateMergeFunction();
            }

            // Reduce
            for (final Object resultFromAGraph : resultsFromAllGraphs) {
                rtn = mergeFunction.apply(resultFromAGraph, rtn);
            }

            return rtn;
        } catch (final Exception e) {
            final List<String> graphIds = graphs.stream().map(GraphSerialisable::getGraphId).collect(Collectors.toList());
            throw new OperationException(String.format("Error while merging results from graphs: %s due to: %s", graphIds, e.getMessage()), e);
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
            mergeFunction = getStoreConfiguredMergeFunction(operation.getPayloadOperation(), context, operation.getGraphIds(), store);
        }

        return mergeFunction;
    }

    private List<GraphSerialisable> getGraphs(final FederatedOperation<INPUT, OUTPUT> operation, final Context context, final FederatedStore store) throws OperationException {
        List<GraphSerialisable> getGraphs;
        try {
            getGraphs = store.getGraphs(context.getUser(), operation.getGraphIds(), operation);
        } catch (final IllegalArgumentException e) {
            throw new OperationException("Error obtaining graph information for operation", e);
        }

        return getGraphs != null ? getGraphs : Collections.emptyList();
    }

    /**
     * Checks if the schemas of the supplied graphs have any of the same groups.
     *
     * @param graphs The list of graphs.
     * @return True if any schema groups are shared.
     */
    private boolean graphsHaveCommonSchemaGroups(final List<GraphSerialisable> graphs) {
        // Check if any of the graphs have common groups in their schemas
        List<Schema> graphSchemas = graphs.stream()
            .map(GraphSerialisable::getSchema)
            .collect(Collectors.toList());

        // Compare all schemas against each other
        for (int i = 0; i < graphSchemas.size() - 1; i++) {
            for (int j = i + 1; j < graphSchemas.size(); j++) {
                // Compare each schema against the others to see if common groups
                Set<String> firstGroupSet = graphSchemas.get(i).getGroups();
                Set<String> secondGroupSet = graphSchemas.get(j).getGroups();
                if (!Collections.disjoint(firstGroupSet, secondGroupSet)) {
                    LOGGER.debug("Found common schema groups between requested graphs");
                    return true;
                }
            }
        }
        return false;
    }
}
