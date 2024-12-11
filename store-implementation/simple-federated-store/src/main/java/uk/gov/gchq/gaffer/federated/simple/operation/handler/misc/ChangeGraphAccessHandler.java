/*
 * Copyright 2024 Crown Copyright
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

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.federated.simple.operation.ChangeGraphAccess;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class ChangeGraphAccessHandler implements OperationHandler<ChangeGraphAccess> {

    @Override
    public Object doOperation(final ChangeGraphAccess operation, final Context context, final Store store) throws OperationException {
        try {
            // Check user for write access as we're modifying the graph
            GraphAccess existingAccess = ((FederatedStore) store).getGraphAccess(operation.getGraphId());
            if (!existingAccess.hasWriteAccess(context.getUser(), store.getProperties().getAdminAuth())) {
                throw new OperationException(
                    "User: '" + context.getUser().getUserId() + "' does not have write permissions for Graph: " + operation.getGraphId());
            }

            // Create the new access object based on what was specified
            GraphAccess.Builder accessBuilder = new GraphAccess.Builder()
                .owner(operation.getOwner() != null ? operation.getOwner() : existingAccess.getOwner())
                .isPublic(operation.isPublic() != null ? operation.isPublic() : existingAccess.isPublic())
                .readAccessPredicate(operation.getReadPredicate() != null ? operation.getReadPredicate() : existingAccess.getReadAccessPredicate())
                .writeAccessPredicate(operation.getWritePredicate() != null ? operation.getWritePredicate() : existingAccess.getWriteAccessPredicate());

            // Update the access
            ((FederatedStore) store).changeGraphAccess(operation.getGraphId(), accessBuilder.build());
        } catch (final CacheOperationException e) {
            throw new OperationException("Error changing the Graph Access for: " + operation.getGraphId(), e);
        }

        return null;
    }

}
