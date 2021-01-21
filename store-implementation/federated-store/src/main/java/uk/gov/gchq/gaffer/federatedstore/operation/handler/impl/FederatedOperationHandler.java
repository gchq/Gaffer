/*
 * Copyright 2020 Crown Copyright
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

import static java.util.Objects.nonNull;

public class FederatedOperationHandler<PAYLOAD extends Operation, O> extends FederationHandler<FederatedOperation, O, PAYLOAD> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederationHandler.class);

    @Override
    PAYLOAD getPayloadOperation(final FederatedOperation operation) {
        Operation payloadOperation = operation.getPayloadOperation();
        //TODO completely tidy up this important logic see FedOp for autoing this.

        if (nonNull(operation.getOptions())) {
            operation.getOptions().forEach((k, v) -> payloadOperation.addOption(k.toString(), v.toString()));
        }

        return (PAYLOAD) payloadOperation;
    }

    @Override
    String getGraphIdsCsv(final FederatedOperation operation) {
        return operation.getGraphIdsCSV();
    }

    protected KorypheBinaryOperator<O> getMergeFunction(final FederatedOperation operation) {
        return operation.getMergeFunction();
    }
}
