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

import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.List;

public class FederatedOperationChainHandler<O> extends FederatedOperationOutputHandler<OperationChain<O>, O> {
    @Override
    protected O mergeResults(final List<O> results, final OperationChain<O> operationChain, final Context context, final Store store) {
        final O rtn;

        final Operation lastOperation = getLastOperation(operationChain);
        if (lastOperation instanceof Output) {
            final Output lastOutput = (Output) lastOperation;
            lastOutput.getOutputTypeReference();
            for (O result : results) {
                //TODOL combine
            }

            throw new UnsupportedOperationException("Not yet implemented");
        } else {
            rtn = null;
        }

        return rtn;
    }

    private Operation getLastOperation(final OperationChain<O> operationChain) {
        final Object[] objects = operationChain.getOperations().toArray();
        return (Operation) objects[objects.length - 1];
    }
}
