/*
 * Copyright 2017-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * A handler for RemoveStore operation for the FederatedStore.
 * <p>
 * Does not delete the graph, just removes it from the scope of the FederatedStore.
 *
 * @see OperationHandler
 * @see FederatedStore
 * @see RemoveStore
 */
public class FederatedRemoveGraphHandler implements OperationHandler<RemoveStore> {
    @Override
    public Void doOperation(final RemoveStore operation, final Context context, final Store store) throws OperationException {
        try {
            ((FederatedStore) store).remove(operation.getId(), context.getUser());
        } catch (final Exception e) {
            throw new OperationException("Error removing graph: " + operation.getId(), e);
        }
        return null;
    }
}
