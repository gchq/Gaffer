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

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.isUserRequestingAdminUsage;

public class FederatedGetAllGraphIDHandler implements OutputOperationHandler<GetAllGraphIds, Iterable<? extends String>> {

    @Override
    public Iterable<? extends String> doOperation(final GetAllGraphIds operation, final Context context, final Store store) throws OperationException {
        try {
            return ((FederatedStore) store).getAllGraphIds(context.getUser(), isUserRequestingAdminUsage(operation));
        } catch (final Exception e) {
            throw new OperationException("Error getting all graphIds", e);
        }
    }
}
