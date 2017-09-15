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
package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;

import java.util.ArrayList;
import java.util.List;

public class FederatedOperationChainHandler<OUT> extends OperationChainHandler<OUT> {

    @Override
    public OUT doOperation(final OperationChain<OUT> operationChain, final Context context, final Store store) throws OperationException {
        OUT result;
        if (store instanceof FederatedStore) {
            result = doForFederatedStore(operationChain, context, (FederatedStore) store);
        } else {
            result = super.doOperation(operationChain, context, store);
        }
        return result;
    }

    private OUT doForFederatedStore(final OperationChain<OUT> operationChain, final Context context, final FederatedStore federatedStore) throws OperationException {
        final ArrayList<OUT> results = doOperationChainForEachGraph(operationChain, context, federatedStore);
        return mergeResults(results, operationChain);
    }

    private OUT mergeResults(final ArrayList<OUT> results, final OperationChain<OUT> operationChain) {
        final OUT rtn;

        final Operation lastOperation = getLastOperation(operationChain);
        if (lastOperation instanceof Output) {
            final Output lastOutput = (Output) lastOperation;
            lastOutput.getOutputTypeReference();
            for (OUT result : results) {
                //combine
            }

            throw new UnsupportedOperationException("Not yet implemented");
        } else {
            rtn = null;
        }

        return rtn;
    }

    private Operation getLastOperation(final OperationChain<OUT> operationChain) {
        final Object[] objects = operationChain.getOperations().toArray();
        return (Operation) objects[objects.length - 1];
    }

    private ArrayList<OUT> doOperationChainForEachGraph(final OperationChain<OUT> operationChain, final Context context, final FederatedStore federatedStore) throws OperationException {
        final ArrayList<OUT> results = Lists.newArrayList();
        for (Graph graph : federatedStore.getAllGraph()) {
            final Store tempStore = Store.createStore(graph.getGraphId(), graph.getSchema(), graph.getStoreProperties());
            final FederatedOperationChainHandler<OUT> outOperationChainHandler = new FederatedOperationChainHandler<>(this.opChainValidator, this.opChainOptimisers);
            final OUT graphResult = outOperationChainHandler.doOperation(operationChain, context, tempStore);
            results.add(graphResult);
        }
        return results;
    }

    public FederatedOperationChainHandler(final OperationChainValidator opChainValidator, final List<OperationChainOptimiser> opChainOptimisers) {
        super(opChainValidator, opChainOptimisers);
    }

}
