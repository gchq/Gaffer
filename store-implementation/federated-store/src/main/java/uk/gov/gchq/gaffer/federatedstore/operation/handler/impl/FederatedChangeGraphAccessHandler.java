/*
 * Copyright 2020-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.federatedstore.FederatedAccess;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphAccess;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

/**
 * A handler for {@link ChangeGraphAccess} operation for the FederatedStore.
 */
public class FederatedChangeGraphAccessHandler implements OutputOperationHandler<ChangeGraphAccess, Boolean> {

    @Override
    public Boolean doOperation(final ChangeGraphAccess operation, final Context context, final Store store) throws OperationException {
        try {
            final FederatedAccess federatedAccess = new FederatedAccess(operation.getGraphAuths(), operation.getOwnerUserId(), operation.getIsPublic());
            final User user = context.getUser();
            return ((FederatedStore) store).changeGraphAccess(user, operation.getGraphId(), federatedAccess, operation.isUserRequestingAdminUsage());
        } catch (final Exception e) {
            throw new OperationException("Error changing graph access", e);
        }
    }
}
