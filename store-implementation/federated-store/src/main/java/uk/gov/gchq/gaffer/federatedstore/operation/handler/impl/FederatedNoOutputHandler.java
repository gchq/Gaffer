/*
 * Copyright 2017-2020 Crown Copyright
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
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.util.stream.Collectors;

import static java.util.Objects.isNull;

/**
 * A handler for GetElements operation for the FederatedStore.
 *
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 */
public class FederatedNoOutputHandler<PAYLOAD extends Operation>
        extends FederationHandler<PAYLOAD, Object, PAYLOAD> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederationHandler.class);

    @Override
    public Object doOperation(final PAYLOAD operation, final Context context, final Store store) throws OperationException {
        LOGGER.debug("{}: before fedOp pipe = {}", store.getGraphId(), isNull(operation.getOptions()) ? null : operation.getOptions().keySet().stream().filter(e -> e.startsWith("FederatedStore.processed.")).collect(Collectors.toList()));
        //TODO ON INNER GRAPH OPTIONS IS MISSING WHY!
        FederatedOperation fedOp = FederatedStoreUtil.getFederatedOperation(operation);

        Object ignore = new FederatedOperationHandler<PAYLOAD, Object>().doOperation(fedOp, context, store);

        operation.setOptions(fedOp.getOptions());

        LOGGER.debug("{}: after fedOp pipe = {}", store.getGraphId(), isNull(operation.getOptions()) ? null : operation.getOptions().keySet().stream().filter(e -> e.startsWith("FederatedStore.processed.")).collect(Collectors.toList()));
        return null;
    }

    @Override
    KorypheBinaryOperator getMergeFunction(final PAYLOAD ignore) {
        throw new IllegalStateException();
    }

    @Override
    PAYLOAD getPayloadOperation(final PAYLOAD operation) {
        throw new IllegalStateException();
    }

    @Override
    String getGraphIdsCsv(final PAYLOAD ignore) {
        throw new IllegalStateException();
    }


}
