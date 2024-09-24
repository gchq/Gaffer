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

package uk.gov.gchq.gaffer.federated.simple.operation.handler.add;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.federated.simple.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_ALLOW_PUBLIC_GRAPHS;

public class AddGraphHandler implements OperationHandler<AddGraph> {

    @Override
    public Void doOperation(final AddGraph operation, final Context context, final Store store) throws OperationException {
        // Create the graph serialisable from the supplied params
        GraphSerialisable newGraph = new GraphSerialisable.Builder()
                .config(operation.getGraphConfig())
                .schema(operation.getSchema())
                .properties(operation.getProperties())
                .build();

        // Init the builder with the owner if set
        GraphAccess.Builder accessBuilder = new GraphAccess.Builder()
            .owner(operation.getOwner() != null ? operation.getOwner() : context.getUser().getUserId());
        // Add options to the graph access
        if (operation.isPublic() != null) {
            accessBuilder.isPublic(operation.isPublic());
        }
        if (operation.getReadPredicate() != null) {
            accessBuilder.readAccessPredicate(operation.getReadPredicate());
        }
        if (operation.getWritePredicate() != null) {
            accessBuilder.writeAccessPredicate(operation.getWritePredicate());
        }
        GraphAccess access = accessBuilder.build();

        // Check if public graphs can be added
        if (access.isPublic() && !Boolean.parseBoolean(store.getProperties().get(PROP_ALLOW_PUBLIC_GRAPHS, "true"))) {
            throw new OperationException("Public graphs are not allowed to be added to this store");
        }

        // Add the graph
        ((FederatedStore) store).addGraph(newGraph, access);

        return null;
    }
}
