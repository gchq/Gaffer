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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.util.HashSet;
import java.util.Map;

import static java.util.Objects.nonNull;

/**
 * FederatedOperation handler for the federation of an PAYLOAD operation with an expected return type OUTPUT.
 *
 * @param <PAYLOAD> The operation to be federated and executed by delegate graphs
 * @param <OUTPUT>  The expected return type of the operation when handled.
 */
public class FederatedOperationHandler<PAYLOAD extends Operation, OUTPUT> extends FederationHandler<FederatedOperation, OUTPUT, PAYLOAD> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederationHandler.class);

    @Override
    PAYLOAD getPayloadOperation(final FederatedOperation operation) {
        Operation payloadOperation = operation.getPayloadOperation();
        payloadOperation = copyOptionToPayload(operation, payloadOperation);

        return (PAYLOAD) payloadOperation;
    }

    private Operation copyOptionToPayload(FederatedOperation operation, Operation payloadOperation) {
        //TODO completely tidy up this important logic see FedOp for auto-ing this.
        if (nonNull(operation.getOptions())) {
            loggingGetPayload(operation, payloadOperation);
            operation.getOptions().forEach((k, v) -> payloadOperation.addOption(k.toString(), v.toString()));
        }
        return payloadOperation;
    }

    private void loggingGetPayload(FederatedOperation operation, Operation payloadOperation) {
        LOGGER.info("copying options from FederationOperation to Payload operation");
        if (LOGGER.isDebugEnabled()) {

            Map operationOptions = operation.getOptions();
            Map<String, String> payloadOptions = payloadOperation.getOptions();
            if (nonNull(operationOptions) && nonNull(payloadOptions)) {
                HashSet<String> intersection = new HashSet<>(operationOptions.keySet());
                intersection.retainAll(payloadOptions.keySet());

                if (!intersection.isEmpty()) {
                    //TODO test
                    intersection.forEach(s -> LOGGER.debug("overwriting {} was:{} now:{}", s, payloadOperation.getOption(s), operation.getOption(s)));
                }
            }
        }
    }

    @Override
    String getGraphIdsCsv(final FederatedOperation operation) {
        return operation.getGraphIdsCSV();
    }

    @Override
    protected KorypheBinaryOperator<OUTPUT> getMergeFunction(final FederatedOperation operation) {
        return operation.getMergeFunction();
    }
}
