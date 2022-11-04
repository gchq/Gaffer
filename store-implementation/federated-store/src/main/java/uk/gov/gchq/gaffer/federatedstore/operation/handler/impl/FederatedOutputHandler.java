/*
 * Copyright 2017-2022 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import static java.util.Objects.isNull;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

/**
 * Handler for the federation of an PAYLOAD operation with an expected return type Iterable
 *
 * @param <PAYLOAD> The operation to be federated and executed by delegate graphs.
 * @param <OUTPUT>  The type of object returned by Federation.
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class FederatedOutputHandler<PAYLOAD extends Output<OUTPUT>, OUTPUT>
        implements OutputOperationHandler<PAYLOAD, OUTPUT> {
    //TODO FS should this be a Hook and change the OperationChain?

    private final OUTPUT defaultEmpty;

    public FederatedOutputHandler(final OUTPUT defaultEmpty) {
        this.defaultEmpty = defaultEmpty;
    }

    public FederatedOutputHandler() {
        this(null);
    }

    @Override
    public OUTPUT doOperation(final PAYLOAD operation, final Context context, final Store store) throws OperationException {
        try {
            if (null == operation) {
                throw new OperationException("Operation cannot be null");
            }

            final OUTPUT output;

            FederatedOperation federatedOperation = getFederatedOperation(operation);

            Object result = store.execute(federatedOperation, context);
            try {
                output = (OUTPUT) result;
            } catch (final ClassCastException e) {
                throw new OperationException(String.format("Could not cast execution result to output type. output:%s result:%s", Iterable.class, result.getClass()), e);
            }
            operation.setOptions(federatedOperation.getOptions());

            return isNull(output) ? defaultEmpty : output;
        } catch (final Exception e) {
            throw new OperationException(String.format("Error federating operation:%s", operation == null ? null : operation.getClass()), e);
        }
    }

}
