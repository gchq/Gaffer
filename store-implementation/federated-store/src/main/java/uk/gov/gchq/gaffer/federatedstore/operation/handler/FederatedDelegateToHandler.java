/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationWithSchemaHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ValidateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.AggregateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.FilterHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.TransformHandler;

import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedWrappedSchema;

public class FederatedDelegateToHandler implements OutputOperationHandler<InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>, Iterable<? extends Element>> {
    private final OutputOperationHandler<InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>, Iterable<? extends Element>> handler;

    public FederatedDelegateToHandler(final OutputOperationHandler<? extends InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>, Iterable<? extends Element>> handler) {
        this.handler = (OutputOperationHandler<InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>, Iterable<? extends Element>>) handler;
    }

    @Override
    public Iterable<? extends Element> doOperation(final InputOutput<Iterable<? extends Element>, Iterable<? extends Element>> operation, final Context context, final Store store) throws OperationException {
        if (TransformHandler.class.isAssignableFrom(handler.getClass())
                || FilterHandler.class.isAssignableFrom(handler.getClass())) {
            return (Iterable<? extends Element>) ((OperationWithSchemaHandler) handler).doOperation(operation, ((FederatedStore) store).getSchema(getFederatedWrappedSchema(), context));
        } else if (ValidateHandler.class.isAssignableFrom(handler.getClass())
                || AggregateHandler.class.isAssignableFrom(handler.getClass())) {
            return (Iterable<? extends Element>) ((OperationWithSchemaHandler) handler).doOperation(operation, ((FederatedStore) store).getSchema(context));
        } else {
            return handler.doOperation(operation, context, store);
        }
    }
}
