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
import uk.gov.gchq.gaffer.federated.simple.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class RemoveGraphHandler implements OperationHandler<RemoveGraph> {

    @Override
    public Object doOperation(final RemoveGraph operation, final Context context, final Store store) throws OperationException {
        try {
            // Check user for write access as we're modifying the graph
            GraphAccess access = ((FederatedStore) store).getGraphAccess(operation.getGraphId());
            if (!access.hasWriteAccess(context.getUser(), store.getProperties().getAdminAuth())) {
                throw new OperationException(
                    "User: '" + context.getUser().getUserId() + "' does not have write permissions for Graph: " + operation.getGraphId());
            }
        } catch (final CacheOperationException e) {
            throw new OperationException(e);
        }

        // If asked to delete all data then run that operation first on the requested graph
        if (operation.getDeleteAllData()) {
            DeleteAllData deleteAllOp = new DeleteAllData.Builder()
                .option(FederatedOperationHandler.OPT_GRAPH_IDS, operation.getGraphId())
                .build();
            store.execute(deleteAllOp, context);
        }

        // Remove the graph from the scope of the store
        ((FederatedStore) store).removeGraph(operation.getGraphId());

        // Nothing to return
        return null;
    }

}
