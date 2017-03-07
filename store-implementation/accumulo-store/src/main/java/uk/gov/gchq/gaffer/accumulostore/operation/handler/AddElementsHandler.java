/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class AddElementsHandler implements OperationHandler<AddElements, Void> {
    @Override
    public Void doOperation(final AddElements operation,
                            final Context context, final Store store)
            throws OperationException {
        addElements(operation, (AccumuloStore) store);
        return null;
    }

    private void addElements(final AddElements addElementsOperation, final AccumuloStore store)
            throws OperationException {
        try {
            store.addElements(addElementsOperation.getElements());
        } catch (final StoreException e) {
            throw new OperationException("Failed to add elements", e);
        }
    }
}
