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
import uk.gov.gchq.gaffer.graph.library.GraphLibrary;
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
        return new OtherGraphExporter(
                context.getUser(),
                context.getJobId(),
                createGraph(export, store)
        );
    }

    private Graph createGraph(ExportToOtherGraph export, final Store store) {
        final String exportGraphId = export.getGraphId();
        if (store.getGraphId().equals(exportGraphId)) {
            throw new IllegalArgumentException("Cannot export to the same graph: " + exportGraphId);
        }

        final GraphLibrary graphLibrary = export.getGraphLibrary();

        // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
        if (null == graphLibrary) {
            final Schema schema = null != export.getSchema() ? export.getSchema() : store.getSchema();
            final StoreProperties properties = null != export.getStoreProperties() ? export.getStoreProperties() : store.getProperties();
            return new Graph.Builder()
                    .graphId(exportGraphId)
                    .addSchema(schema)
                    .storeProperties(properties)
                    .build();
        }

        // If the graphId exists in the graphLibrary then just use it
        if (graphLibrary.exists(exportGraphId)) {
            return new Graph.Builder()
                    .graphId(exportGraphId)
                    .library(graphLibrary)
                    .build();
        } else {
            Schema schema = null;
            if (null != export.getParentSchemaId()) {
                schema = graphLibrary.getSchema(export.getParentSchemaId());
            }
            if (null != export.getSchema()) {
                if (null == schema) {
                    schema = export.getSchema();
                } else
                    // delete the old schema id as we are about to modify the schema
                    schema = new Schema.Builder()
                            .id(null)
                            .merge(export.getSchema())
                            .build();
            }
            if (null == schema) {
                // as a last resort just use the schema from the current store
                schema = store.getSchema();
            }

            StoreProperties storeProperties = null;
            if (null != export.getParentStorePropertiesId()) {
                storeProperties = graphLibrary.getProperties(export.getParentStorePropertiesId());
            }
            if (null != export.getStoreProperties()) {
                if (null == storeProperties) {
                    storeProperties = export.getStoreProperties();
                } else {
                    // delete the old properties id as we are about to modify the properties
                    storeProperties.setId(null);
                    storeProperties.getProperties().putAll(export.getStoreProperties().getProperties());
                }
            }
            if (null == storeProperties) {
                // as a last resort just use the properties from the current store
                storeProperties = store.getProperties();
            }

            return new Graph.Builder()
                    .graphId(exportGraphId)
                    .library(graphLibrary)
                    .addSchema(schema)
                    .storeProperties(storeProperties)
                    .build();
        }
    }
}
