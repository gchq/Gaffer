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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

/**
 * Operation handler for the federation of an PAYLOAD operation with an expected return type of Void/Null.
 * <p>
 * The Operation with no output is wrapped in a defaulted FederatedOperation and re-executed.
 *
 * @param <PAYLOAD> The operation to be federated and executed by delegate graphs.
 */
@Since("2.0.0")
public class FederatedNoOutputHandler<PAYLOAD extends Operation> extends FederationHandler<PAYLOAD, Object, PAYLOAD> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederationHandler.class);

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
    public Object doOperation(final PAYLOAD operation, final Context context, final Store store) throws OperationException {
        loggingIsProcessedByFederatedStore(operation, store, "before");
        FederatedOperation fedOp = getFederatedOperation(operation);

        //TODO Handle directly or re-send back to Store
        Object ignore = new FederatedOperationHandler<PAYLOAD, Object>().doOperation(fedOp, context, store);

        //TODO review Options
        operation.setOptions(fedOp.getOptions());

        loggingIsProcessedByFederatedStore(operation, store, "after");
        //TODO null or void?
        return null;
    }

    private void loggingIsProcessedByFederatedStore(PAYLOAD operation, Store store, String when) {
        if (LOGGER.isDebugEnabled()) {
            Object o = isNull(operation.getOptions()) ? null : operation.getOptions().keySet().stream().filter(e -> e.startsWith("FederatedStore.processed.")).collect(Collectors.toList());
            LOGGER.debug("{}: {} fedOp pipe = {}", store.getGraphId(), when, o);
        }
    }

    @Override
    KorypheBinaryOperator<Object> getMergeFunction(final PAYLOAD ignore) {
        throw new IllegalStateException();
    }

    @Override
    PAYLOAD getPayloadOperation(final PAYLOAD ignore) {
        throw new IllegalStateException();
    }

    @Override
    String getGraphIdsCsv(final PAYLOAD ignore) {
        throw new IllegalStateException();
    }


}
