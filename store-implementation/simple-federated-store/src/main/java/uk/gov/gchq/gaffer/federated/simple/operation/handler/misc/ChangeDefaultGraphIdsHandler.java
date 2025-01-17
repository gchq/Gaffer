/*
 * Copyright 2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation.handler.misc;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.operation.ChangeDefaultGraphIds;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;

import static uk.gov.gchq.gaffer.federated.simple.FederatedStore.FEDERATED_STORE_SYSTEM_USER;

public class ChangeDefaultGraphIdsHandler implements OperationHandler<ChangeDefaultGraphIds> {

    @Override
    public Object doOperation(final ChangeDefaultGraphIds operation, final Context context, final Store store) throws OperationException {
        User user = context.getUser();
        String adminAuth = store.getProperties().getAdminAuth();
        // Check is user is admin as we're modifying the federated store, also allow if no admin auth is set
        if ((user.getUserId().equals(FEDERATED_STORE_SYSTEM_USER))
                || adminAuth.isEmpty()
                || user.getOpAuths().contains(adminAuth)) {
            ((FederatedStore) store).setDefaultGraphIds(operation.geGraphIds());
        } else {
            throw new OperationException("User: '" + user.getUserId() + "' does not have permission to change default Graph IDs");
        }

        return null;
    }
}
