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
import uk.gov.gchq.gaffer.graph.library.GraphLibrary;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.export.ExportToHandler;
import java.nio.file.Files;
import java.nio.file.Paths;

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
        String storePropertiesId;
        String schemaId;
        GraphLibrary graphLibrary = new FileGraphLibrary(export.getGraphLibraryPath());

        // Create new graphLibrary using the given path
        // If graphLibrary with the given graphId already exist we must make sure this graphLibrary is not overwritten!!!
        // Just use the same graphLibrary.
        if (graphLibrary.exists(export.getGraphId())) {
            return new Graph.Builder()
                    .graphId(export.getGraphId())
                    .library(graphLibrary)
                    .build();
        } else {

            // Else the graphLibrary with the export.graphId does not exist and we can do what we want to the graphLibrary.
            // Just because we have checked the schema and properties are not related to the given graphId there is nothing to say they
            // don't already exist for a different graphId but in the same path specified.  We must check these do not exist before we start
            // recreating them to make sure we don't overwrite them.
            if (export.getStoreProperties().getId() != null) {
                storePropertiesId = export.getStoreProperties().getId();
            } else {
                throw new IllegalArgumentException("No id is defined in the provided StoreProperties");
            }

            if (export.getSchema().getId() != null) {
                schemaId = export.getSchema().getId();
            } else {
                throw new IllegalArgumentException("No id is set within the provided Schema");
            }

            if (!Files.exists(Paths.get(export.getGraphLibraryPath() + "/" + schemaId + "Schema.json"))
                    && !Files.exists(Paths.get(export.getGraphLibraryPath() + "/" + storePropertiesId + "Props.properties"))) {
                // we have now checked nothing to do with any of the id's exist so we can do what we like
                // and add the new schema and storeProperties to the graphLibrary
                graphLibrary.add(export.getGraphId(), export.getSchema(), export.getStoreProperties());
            } else {
                // either the schemaId or the storePropertiesId already has a file related so we can just addOrUpdate
                graphLibrary.addOrUpdate(export.getGraphId(), export.getSchema(), export.getStoreProperties());
            }

            return new Graph.Builder()
                    .graphId(export.getGraphId())
                    .library(graphLibrary)
                    .addSchema(export.getSchema())
                    .storeProperties(export.getStoreProperties())
                    .build();
        }
    }
}
