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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import static java.util.Objects.isNull;

/**
 * Operation handler for the federation of an PAYLOAD operation with an expected return type CloseableIterable<ITERABLE_ELEMENTS>
 *
 * @param <PAYLOAD>           The operation to be federated and executed by delegate graphs.
 * @param <ITERABLE_ELEMENTS> the type of elements returned by the Output Iterable
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class FederatedOutputCloseableIterableHandler<PAYLOAD extends Output<? extends CloseableIterable<? extends ITERABLE_ELEMENTS>>, ITERABLE_ELEMENTS>
        extends FederationHandler<PAYLOAD, CloseableIterable<? extends ITERABLE_ELEMENTS>, PAYLOAD>
        implements OutputOperationHandler<PAYLOAD, CloseableIterable<? extends ITERABLE_ELEMENTS>> {

    @Override
    public CloseableIterable<? extends ITERABLE_ELEMENTS> doOperation(final PAYLOAD operation, final Context context, final Store store) throws OperationException {

        FederatedOperation fedOp = FederatedStoreUtil.getFederatedOperation(operation);

        //TODO REVIEW RETURN TYPE
        CloseableIterable<? extends ITERABLE_ELEMENTS> results = new FederatedOperationHandler<PAYLOAD, CloseableIterable<? extends ITERABLE_ELEMENTS>>().doOperation(fedOp, context, store);

        //TODO Review SetOptions
        operation.setOptions(fedOp.getOptions());

        return isNull(results) ? new EmptyClosableIterable<ITERABLE_ELEMENTS>() : results;
    }


    @Override
    KorypheBinaryOperator getMergeFunction(final PAYLOAD ignore) {
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
