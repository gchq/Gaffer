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
package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.TransformHandler;

public class FederatedTransformHandler implements OutputOperationHandler<Transform, Iterable<? extends Element>> {
    private final TransformHandler handler;

    public FederatedTransformHandler() {
        this(new TransformHandler());
    }

    public FederatedTransformHandler(final TransformHandler handler) {
        this.handler = handler;
    }

    @Override
    public Iterable<? extends Element> doOperation(final Transform operation,
                                                   final Context context,
                                                   final Store store)
            throws OperationException {
        return handler.doOperation(operation, ((FederatedStore) store).getSchema(operation, context));
    }
}
