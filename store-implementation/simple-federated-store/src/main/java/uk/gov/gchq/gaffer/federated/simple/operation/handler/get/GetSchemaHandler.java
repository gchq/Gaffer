/*
 * Copyright 2024-2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation.handler.get;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOutputHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.exception.VertexSerialiserSchemaException;
import uk.gov.gchq.gaffer.store.schema.exception.VisibilityPropertySchemaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple handler for merging schemas from multiple graphs.
 */
public class GetSchemaHandler extends FederatedOutputHandler<GetSchema, Schema> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetSchemaHandler.class);

    @Override
    public Schema doOperation(final GetSchema operation, final Context context, final Store store) throws OperationException {
        List<Graph> graphsToExecute = this.getGraphsToExecuteOn(operation, context, (FederatedStore) store);

        if (graphsToExecute.isEmpty()) {
            return new Schema();
        }

        // Execute the operation chain on each graph
        List<Schema> graphResults = new ArrayList<>();
        for (final Graph graph : graphsToExecute) {
            graphResults.add(graph.execute(operation, context.getUser()));
        }

        return getMergedSchema(graphResults);
    }

    /**
     * Shortcut to running the operation if you already have the graphs you want
     * to run on.
     *
     * @param operation The operation.
     * @param context The context.
     * @param graphsToExecute The list of graphs to execute on.
     * @return The merged schema.
     * @throws OperationException If issue.
     */
    public Schema doOperationOnGraphs(
            final GetSchema operation,
            final Context context,
            final List<GraphSerialisable> graphsToExecute) throws OperationException {
        if (graphsToExecute.isEmpty()) {
            return new Schema();
        }

        // Execute the operation chain on each graph
        List<Schema> graphResults = new ArrayList<>();
        for (final GraphSerialisable gs : graphsToExecute) {
            graphResults.add(gs.getGraph().execute(operation, context.getUser()));
        }

        return getMergedSchema(graphResults);
    }

    /**
     * Merges all the supplied graph schemas together and returns the result.
     *
     * @param schemas The list of schemas
     * @return The merged schema
     */
    public static Schema getMergedSchema(final List<Schema> schemas) {
        // Merge schemas using schema builder
        boolean wipeVisabilityProperty = false;
        boolean wipeVertexSerialiser = false;
        Schema.Builder mergeSchema = new Schema.Builder();

        for (final Schema schema : schemas) {
            try {
                mergeSchema.merge(schema);
            } catch (final VisibilityPropertySchemaException e) {
                LOGGER.warn("Schema visibility properties are conflicting, continuing to merge sub graph schemas without");
                mergeSchema.merge(new Schema.Builder(schema).visibilityProperty(null).build());
                wipeVisabilityProperty = true;
            } catch (final VertexSerialiserSchemaException e) {
                LOGGER.warn("Vertex serialisers are conflicting, continuing to merge sub graph schemas without");
                mergeSchema.merge(new Schema.Builder(schema).vertexSerialiser(null).build());
                wipeVertexSerialiser = true;
            }
        }

        if (wipeVisabilityProperty) {
            mergeSchema.visibilityProperty(null);
        }
        if (wipeVertexSerialiser) {
            mergeSchema.vertexSerialiser(null);
        }

        return mergeSchema.build();
    }
}
