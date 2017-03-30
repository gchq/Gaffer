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
package uk.gov.gchq.gaffer.mapstore.operation.handler;

import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * An {@link OperationHandler} for the {@link AddElements} operation on the {@link MapStore}. It simply delegates
 * the operation to the {@link MapStore#addElements} method.
 */
public class AddElementsHandler implements OperationHandler<AddElements, Void> {

    @Override
    public Void doOperation(final AddElements addElements, final Context context, final Store store) throws OperationException {
        doOperation(addElements, (MapStore) store);
        return null;
    }

    private void doOperation(final AddElements addElements, final MapStore mapStore) {
        mapStore.addElements(addElements.getElements());
    }
}
