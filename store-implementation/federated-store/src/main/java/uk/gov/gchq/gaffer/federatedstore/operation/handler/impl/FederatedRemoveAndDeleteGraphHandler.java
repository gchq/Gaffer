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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;

import java.util.Collections;
import java.util.List;


/**
 * A handler for RemoveGraph operation for the FederatedStore.
 * And delete associated Accumulo Tables.
 * <p>
 *
 * @see FederatedStore
 * @see RemoveGraph
 */
public class FederatedRemoveAndDeleteGraphHandler extends FederatedRemoveGraphHandler {

    @Override
    public Boolean doOperation(final RemoveGraph operation, final Context context, final Store store) throws OperationException {
        try {
            //Get the graph before they are removed.
            final List<Graph> graphsToRemove = ((FederatedStore) store).getGraphs(context.getUser(), Collections.singletonList(operation.getGraphId()), operation);

            //Ask graphs to delete all data.
            for (final Graph graph : graphsToRemove) {
                if (graph.isSupported(DeleteAllData.class)) {
                    graph.execute(new DeleteAllData(), context);
                }
            }

            //Remove graphs from Federation
            final boolean removed = super.doOperation(operation, context, store);

            return removed;
        } catch (final Exception e) {
            throw new OperationException(String.format("Error deleting accumulo table: %s", operation.getGraphId()), e);
        }
    }
}
