/*
 * Copyright 2018-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.getSkipFailedFederatedStoreExecute;

public abstract class FederationHandler<OP extends Operation, OUTPUT, PAYLOAD extends Operation> implements OperationHandler<OP> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederationHandler.class);


    @Override
    public OUTPUT doOperation(final OP operation, final Context context, final Store store) throws OperationException {
        final List<OUTPUT> allGraphResults = getAllGraphResults(operation, context, (FederatedStore) store);
        KorypheBinaryOperator<OUTPUT> mergeFunction = getMergeFunction(operation);

        return mergeResults(allGraphResults, mergeFunction);

    }

    abstract KorypheBinaryOperator<OUTPUT> getMergeFunction(OP operation);

    private List<OUTPUT> getAllGraphResults(final OP operation, final Context context, final FederatedStore store) throws OperationException {
        if (Store.oHOLLA) {
            LOGGER.info("{}:getAllGraphResults():{}", store.getGraphId(), store.getId());
        }
        try {
            List<OUTPUT> results;
            if (Store.oHOLLA) {
                LOGGER.debug("{} options before getGraphs = {}", operation.getClass().getSimpleName(), isNull(operation.getOptions()) ? null : operation.getOptions()); //.keySet().stream()/*.filter(e -> e.startsWith("FederatedStore.processed."))*/.collect(Collectors.toList()));
            }
            final Collection<Graph> graphs = getGraphs(operation, context, store);
            if (Store.oHOLLA) {
                LOGGER.debug("{} options after getGraphs= {}", operation.getClass().getSimpleName(), operation.getOptions()); //.keySet().stream()/*.filter(e -> e.startsWith("FederatedStore.processed."))*/.collect(Collectors.toList()));
            }
            results = new ArrayList<>(graphs.size());
            for (final Graph graph : graphs) {

                if (Store.oHOLLA) {
                    LOGGER.debug("{} double check = {}", operation.getClass().getSimpleName(), operation.getOptions()); //.keySet().stream()/*.filter(e -> e.startsWith("FederatedStore.processed."))*/.collect(Collectors.toList()));
                }

                final Operation updatedOp = FederatedStoreUtil.updateOperationForGraph(getPayloadOperation(operation), graph);
                if (null != updatedOp) {
                    OUTPUT execute = null;
                    try {
                        if (updatedOp instanceof Output) {
                            //TODO review this output distinction.
                            execute = graph.execute((Output<OUTPUT>) updatedOp, context);
                        } else {
                            if (Store.oHOLLA) {
                                LOGGER.debug("delegate graph = {}", graph.getGraphId());
//                                LOGGER.debug("clone ID before = {}", updatedOp.getOptions().keySet().stream().filter(e -> e.startsWith("FederatedStore.processed.")).collect(Collectors.toList()));
                                LOGGER.debug("clone ID before = {}", updatedOp.getOptions());
                                LOGGER.debug("execute non-output");
                                LOGGER.debug("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                            }
                            graph.execute(updatedOp, context);
                            if (Store.oHOLLA) {
                                LOGGER.debug("finished non-output");
                                LOGGER.debug("clone ID after = {}", updatedOp.getOptions());
                            }
                        }
                    } catch (final Exception e) {
                        //TODO make optional argument.
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

    abstract PAYLOAD getPayloadOperation(final OP operation);

    protected OUTPUT mergeResults(final List<OUTPUT> results, final KorypheBinaryOperator<OUTPUT> mergeFunction) throws OperationException {
        try {
            OUTPUT rtn = null;
            if (nonNull(results) && !isEmpty(results)) {
                for (final OUTPUT result : results) {
                    //TODO Test voids & Nulls
                    rtn = mergeFunction.apply(rtn, result);
                }
            }
            return rtn;
        } catch (final Exception e) {
            throw new OperationException("Error while merging results. " + e.getMessage(), e);
        }
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

    abstract String getGraphIdsCsv(OP operation);
}
