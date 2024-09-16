/*
 * Copyright 2017-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.HashSet;
import java.util.Set;

/**
 * A handler for RemoveGraph operation for the FederatedStore.
 * <p>
 * Does not delete the graph, just removes it from the scope of the FederatedStore.
 *
 * @see FederatedStore
 * @see RemoveGraph
 */
public class FederatedRemoveGraphHandler implements OutputOperationHandler<RemoveGraph, Boolean> {
    @Override
    public Boolean doOperation(final RemoveGraph operation, final Context context, final Store store) throws OperationException {
        boolean removed;
        Set<Class<? extends Operation>> originalSupportedOperations = getSupportedOperationsFromSubGraphs(context, (FederatedStore) store);
        try {
            removed = ((FederatedStore) store).remove(operation.getGraphId(), context.getUser(), operation.isRemoveCache(), operation.isUserRequestingAdminUsage());
        } catch (final Exception e) {
            throw new OperationException("Error removing graph: " + operation.getGraphId(), e);
        }
        if (removed) {
            Set<Class<? extends Operation>> updatedSupportedOperations = getSupportedOperationsFromSubGraphs(context, (FederatedStore) store);
            if (!originalSupportedOperations.equals(updatedSupportedOperations)) {
                // unsupportedOperations set is original ops with current ops removed
                originalSupportedOperations.removeAll(updatedSupportedOperations);
                Set<Class<? extends Operation>> unsupportedOperations = originalSupportedOperations;
                for (final Class<? extends Operation> removedOperation : unsupportedOperations) {
                    // externallySupportedOperations only contains operations added by AddGraph
                    // so will never contain FederatedStore core operations.
                    // Therefore, we can remove operation handlers that are in externallySupportedOperations
                    if (((FederatedStore) store).getExternallySupportedOperations().contains(removedOperation)) {
                        store.addOperationHandler(removedOperation, null);
                        ((FederatedStore) store).removeExternallySupportedOperation(removedOperation);
                    }
                }
            }
        }
        return removed;
    }

    private Set<Class<? extends Operation>> getSupportedOperationsFromSubGraphs(final Context context, final FederatedStore store) {
        Set<Class<? extends Operation>> supportedOperations = new HashSet<>();
        for (final GraphSerialisable graphSerialisable : store.getGraphs(context.getUser(), null, new RemoveGraph())) {
            Graph subGraph = graphSerialisable.getGraph();
            supportedOperations.addAll(subGraph.getSupportedOperations());
        }
        return supportedOperations;
    }
}
