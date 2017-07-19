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
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.export.ExportToHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

public class ExportToOtherGraphHandler extends ExportToHandler<ExportToOtherGraph, OtherGraphExporter> {
    private static final String ID = "gaffer.store.id";

    @Override
    protected Class<OtherGraphExporter> getExporterClass() {
        return OtherGraphExporter.class;
    }

    @Override
    protected OtherGraphExporter createExporter(final ExportToOtherGraph export, final Context context, final Store store) {

        return new OtherGraphExporter(
                context.getUser(),
                context.getJobId(),
                createGraph(export, store));
    }

    protected Graph createGraph(final ExportToOtherGraph export, final Store store) {

        final String exportGraphId = export.getGraphId();
        final Schema exportSchema = export.getSchema();
        final StoreProperties exportStoreProperties = export.getStoreProperties();
        final String exportParentSchemaId = export.getParentSchemaId();
        final String exportParentStorePropertiesId = export.getParentStorePropertiesId();

        final String storeGraphId = store.getGraphId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final StoreProperties storeStoreProperties = store.getProperties();
        final Schema storeSchema = store.getSchema();

        ValidationResult validationResult = validate(exportGraphId, exportParentSchemaId, exportParentStorePropertiesId,
                graphLibrary, exportSchema, exportStoreProperties, store);
        if (!validationResult.isValid()) {
            throw new IllegalArgumentException(validationResult.getErrorString());
        }

        StoreProperties storeProperties;
        Schema schema;

        // No store graph library so we create a new Graph
        if (null == graphLibrary) {
            schema = null != exportSchema ? exportSchema : storeSchema;
            final StoreProperties properties = null != exportStoreProperties ? exportStoreProperties : storeStoreProperties;
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
            storeProperties = null;
            if (export.getParentStorePropertiesId() != null) {
                storeProperties = graphLibrary.getProperties(export.getParentStorePropertiesId());
            }
            if (export.getStoreProperties() != null) {
                if (storeProperties == null) {
                    storeProperties = export.getStoreProperties();
                } else {
                    // delete the old properties id as we are about to modify the properties
                    storeProperties.getProperties().remove(ID);
                    storeProperties.getProperties().putAll(export.getStoreProperties().getProperties());
                }
            }
            if (storeProperties == null) {
                storeProperties = store.getProperties();
            }

            schema = null;
            if (export.getParentSchemaId() != null) {
                schema = graphLibrary.getSchema(export.getParentSchemaId());
            }
            if (export.getSchema() != null) {
                if (schema == null) {
                    schema = export.getSchema();
                } else {
                    // delete the old schema id as we are about to modify the schema
                    schema = new Schema.Builder()
                            .merge(schema)
                            .id(null)
                            .merge(export.getSchema())
                            .build();
                }
            }
            if (null == schema) {
                schema = store.getSchema();
            }
        }

        return new Graph.Builder()
                .graphId(exportGraphId)
                .library(graphLibrary)
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
    }

    public ValidationResult validate(final String exportGraphId, final String exportParentSchemaId,
                                     final String exportParentStorePropertiesId, final GraphLibrary storeGraphLibrary,
                                     final Schema exportSchema, final StoreProperties exportStoreProperties,
                                     final Store store) {

        final ValidationResult result = new ValidationResult();

        if (store.getGraphId().equals(exportGraphId)) {
            result.addError("Cannot export to the same graph: " + exportGraphId);
        }
        if (null == storeGraphLibrary) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != exportParentSchemaId) {
                result.addError("parentSchemaId cannot be used without a GraphLibrary");
            }
            if (null != exportParentStorePropertiesId) {
                result.addError("parentStorePropertiesId cannot be used without a GraphLibrary");
            }
        } else if (storeGraphLibrary.exists(exportGraphId)) {
            if (null != exportParentSchemaId) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot use a different schema. Do not set the parentSchemaId field");
            }
            if (null != exportSchema) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot provide a different schema. Do not set the schema field.");
            }
            if (null != exportParentStorePropertiesId) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field");
            }
            if (null != exportStoreProperties) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot provide different store properties. Do not set the storeProperties field.");
            }
        } else {
            if (null != exportParentSchemaId) {
                if (null == store.getGraphLibrary().getSchema(exportParentSchemaId)) {
                    result.addError("Schema could not be found in the graphLibrary with id: " + exportParentSchemaId);
                }
            }
            if (null != exportParentStorePropertiesId) {
                if (null == store.getGraphLibrary().getProperties(exportParentStorePropertiesId)) {
                    result.addError("Store properties could not be found in the graphLibrary with id: " + exportParentStorePropertiesId);
                }
            }
        }
        return result;
    }
}
