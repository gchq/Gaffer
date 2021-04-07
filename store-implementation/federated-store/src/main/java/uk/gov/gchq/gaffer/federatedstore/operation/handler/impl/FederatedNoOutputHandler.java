/*
 * Copyright 2017-2021 Crown Copyright
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

import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.koryphe.Since;

import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

/**
 * Operation handler for the federation of an PAYLOAD operation with an expected return type of Void/Null.
 * <p>
 * The Operation with no output is wrapped in a defaulted FederatedOperation and re-executed.
 *
 * @param <PAYLOAD> The operation to be federated and executed by delegate graphs.
 */
@Since("2.0.0")
public class FederatedNoOutputHandler<PAYLOAD extends Operation> implements OperationHandler<PAYLOAD> {

    /**
     * The Operation with no output is wrapped in a defaulted FederatedOperation and re-executed.
     *
     * @param operation no output operation to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore} the operation should be run on.
     * @return null, no output.
     * @throws OperationException thrown if the operation fails
     */
    @Override
    public Void doOperation(final PAYLOAD operation, final Context context, final Store store) throws OperationException {
        FederatedOperation<Object, Void> fedOp = getFederatedOperation(operation);

        Object ignore = store.execute(fedOp, context);

        //TODO FS Examine, setOptions 1/3
        operation.setOptions(fedOp.getOptions());

        return null;
    }
}
