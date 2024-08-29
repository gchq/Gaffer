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

package uk.gov.gchq.gaffer.federated.simple.operation.handler;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple handler for merging schemas from multiple graphs.
 */
public class GetSchemaHandler extends FederatedOutputHandler<GetSchema, Schema> {

    @Override
    public Schema doOperation(final GetSchema operation, final Context context, final Store store) throws OperationException {
        List<GraphSerialisable> graphsToExecute = this.getGraphsToExecuteOn((FederatedStore) store, operation);

        if (graphsToExecute.isEmpty()) {
            return new Schema();
        }

        // Execute the operation chain on each graph
        List<Schema> graphResults = new ArrayList<>();
        for (final GraphSerialisable gs : graphsToExecute) {
            graphResults.add(gs.getGraph().execute(operation, context.getUser()));
        }

        Schema.Builder mergeSchema = new Schema.Builder();
        graphResults.forEach(mergeSchema::merge);

        return mergeSchema.build();
    }
}
