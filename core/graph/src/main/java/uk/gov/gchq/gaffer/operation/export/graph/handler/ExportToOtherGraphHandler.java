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
    private GraphLibrary storeLibrary;
    private String graphId;

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

        ValidationResult validationResult = validate(export, store);
        if (!validationResult.isValid()) {
            throw new IllegalArgumentException(validationResult.getErrorString());
        }

        storeLibrary = store.getGraphLibrary();
        graphId = export.getGraphId();

        StoreProperties storeProperties;
        Schema schema;

        final String exportGraphId = export.getGraphId();
        if (store.getGraphId().equals(exportGraphId)) {
            throw new IllegalArgumentException("Cannot export to the same graph: " + exportGraphId);
        }

        // No store graph library so we create a new Graph
        if (null == storeLibrary) {
            schema = null != export.getSchema() ? export.getSchema() : store.getSchema();
            final StoreProperties properties = null != export.getStoreProperties() ? export.getStoreProperties() : store.getProperties();
            return new Graph.Builder()
                    .graphId(exportGraphId)
                    .addSchema(schema)
                    .storeProperties(properties)
                    .build();
        }

        // If the graphId exists in the graphLibrary then just use it
        if (storeLibrary.exists(exportGraphId)) {
            return new Graph.Builder()
                    .graphId(exportGraphId)
                    .library(storeLibrary)
                    .build();
        } else {
            storeProperties = null;
            if (export.getParentStorePropertiesId() != null) {
                storeProperties = storeLibrary.getProperties(export.getParentStorePropertiesId());
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
            if (null == storeProperties) {
                storeProperties = export.getStoreProperties();
            }

            schema = null;
            if (export.getParentSchemaId() != null) {
                schema = storeLibrary.getSchema(export.getParentSchemaId());
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
                .library(storeLibrary)
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
    }

    public ValidationResult validate(final ExportToOtherGraph export, final Store store) {

        final ValidationResult result = new ValidationResult();

        if (null == store.getGraphLibrary()) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != export.getParentSchemaId()) {
                result.addError("parentSchemaId cannot be used without a GraphLibrary");
            }
            if (null != export.getParentStorePropertiesId()) {
                result.addError("parentStorePropertiesId cannot be used without a GraphLibrary");
            }
        } else if (store.getGraphLibrary().exists(graphId)) {
            if (null != export.getParentSchemaId()) {
                throw new IllegalArgumentException("GraphId " + graphId + " already exists so you cannot use a different schema. Do not set the parentSchemaId field");
            }
            if (null != export.getSchema()) {
                throw new IllegalArgumentException("GraphId " + graphId + "already exists so you cannot provide a different schema. Do not set the schema field.");
            }
            if (null != export.getParentStorePropertiesId()) {
                throw new IllegalArgumentException("GraphId " + graphId + " already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field");
            }
            if (null != export.getStoreProperties()) {
                throw new IllegalArgumentException("GraphId " + graphId + " already exists so you cannot provide different store properties. Do not set the storeProperties field.");
            }
        } else {
            if (null != export.getParentSchemaId()) {
                final Schema parentSchema = store.getGraphLibrary().getSchema(export.getParentSchemaId());
                if (null == parentSchema) {
                    throw new IllegalArgumentException("Schema could not be found in the graphLibrary with id: " + export.getParentSchemaId());
                }
            }
            if (null != export.getParentStorePropertiesId()) {
                final StoreProperties parentStoreProperties = store.getGraphLibrary().getProperties(export.getParentStorePropertiesId());
                if (null == parentStoreProperties) {
                    throw new IllegalArgumentException("Store properties could not be found in the graphLibrary with id: " + export.getParentStorePropertiesId());
                }
            }
        }
        return result;
    }
}
