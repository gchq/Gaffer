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

public class AddGraphHandler implements OperationHandler<AddGraph> {

    @Override
    public Void doOperation(final AddGraph operation, final Context context, final Store store) throws OperationException {
        // Create the graph serialisable from the supplied params
        GraphSerialisable newGraph = new GraphSerialisable.Builder()
                .config(operation.getGraphConfig())
                .schema(operation.getSchema())
                .properties(operation.getProperties())
                .build();

        GraphAccess access = operation.getGraphAccess() != null
            ? operation.getGraphAccess()
            : new GraphAccess.Builder().owner(context.getUser().getUserId()).build();

        // Add the graph
        ((FederatedStore) store).addGraph(newGraph, access);

        return null;
    }
}
