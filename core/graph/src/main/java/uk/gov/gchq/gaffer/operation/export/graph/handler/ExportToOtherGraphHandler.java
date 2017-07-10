/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export.graph.handler;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.export.ExportToHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class ExportToOtherGraphHandler extends ExportToHandler<ExportToOtherGraph, OtherGraphExporter> {

    @Override
    protected Class<OtherGraphExporter> getExporterClass() {
        return OtherGraphExporter.class;
    }

    @Override
    protected OtherGraphExporter createExporter(final ExportToOtherGraph export, final Context context, final Store store) {
        if (export.getGraphId() == null) {
            throw new IllegalArgumentException("GraphId is required");
        }
        return new OtherGraphExporter(
                context.getUser(),
                context.getJobId(),
                createGraph(export));
    }

    private Graph createGraph(ExportToOtherGraph export) {
        Schema schema = null;
        StoreProperties storeProperties = null;

        // Create new graphLibrary using the given path
        final FileGraphLibrary graphLibrary = new FileGraphLibrary(export.getGraphLibraryPath());

        // Get the schemaId within the graphLibrary using the graphId
        final String schemaId = graphLibrary.getIds(export.getGraphId()).getFirst();

        // Get the storePropertiesId within the graphLibrary using the graphId
        final String storePropertiesId = graphLibrary.getIds(export.getGraphId()).getSecond();

        // If schemaId exists
        if (!schemaId.isEmpty() || schemaId != null) {
            // If the schema related to the schemaId is not null and the export schema is not supplied
            if (graphLibrary.getSchema(schemaId) != null && export.getSchema() == null) {
                // Set the schema to be the one from the graphLibrary
                schema = graphLibrary.getSchema(schemaId);
            }
            // else if schema related to schemaId is null or export schema is supplied
            else if (graphLibrary.getSchema(schemaId) == null || export.getSchema() != null) {
                // create a new schema merging the export schema and the graphLibrary schema
                schema = new Schema.Builder()
                        .merge(export.getSchema())
                        .merge(graphLibrary.getSchema(schemaId))
                        .build();
            } else {
                throw new IllegalArgumentException("No specific schema or GraphLibrary path is supplied");
            }
        }

        // If export supplied storePropertiesId exists
        if (export.getStorePropertiesId() != null || !export.getStorePropertiesId().isEmpty()) {
            // If storeProperties exist in the graphLibrary with the export storePropertiesId
            if (graphLibrary.getProperties(export.getStorePropertiesId()) != null) {
                // Set storeProperties to the graphLibrary storeProperties matching the export storePropertiesId
                storeProperties = graphLibrary.getProperties(export.getStorePropertiesId());
            }
            // Else if the storePropertiesId related to the graphLibrary is not empty
        } else if (!storePropertiesId.isEmpty()) {
            // If the storeProperties related to the storePropertiesId is not null and there is no export storeProperties supplied
            if (graphLibrary.getProperties(storePropertiesId) != null && export.getStoreProperties() == null) {
                // Set the storeProperties to the properties within the graphLibrary
                storeProperties = graphLibrary.getProperties(storePropertiesId);
                // Else if the storeProperties related to the storePropertiesId is null or export storeProperties are supplied
            } else if (graphLibrary.getProperties(storePropertiesId) == null || export.getStoreProperties() != null) {
                // Set the storeProperties to the properties supplied in the export
                storeProperties = export.getStoreProperties();
            } else {
                throw new IllegalArgumentException("No specific storeProperties supplied and none matching the graphId");
            }

        }

        if (schema != null && storeProperties != null) {
            return new Graph.Builder()
                    .graphId(export.getGraphId())
                    .storeProperties(storeProperties)
                    .addSchema(schema)
                    .build();
        } else {
            throw new IllegalArgumentException("Schema or storeProperties is null");
        }
    }
}
