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
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.export.ExportToHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;
import java.util.List;

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

    protected Graph createGraph(final ExportToOtherGraph<?> export, final Store store) {
        validationHandling(store, export);

        final String exportGraphId = export.getGraphId();
        final Schema exportSchema = export.getSchema();
        final StoreProperties exportStoreProperties = export.getStoreProperties();
        final List<String> exportParentSchemaIds = export.getParentSchemaIds();
        final String exportParentStorePropertiesId = export.getParentStorePropertiesId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final StoreProperties storeStoreProperties = store.getProperties();
        final Schema storeSchema = store.getSchema();
        final Graph rtn;

        if (null == graphLibrary) {
            rtn = getGraphWithoutLibrary(exportGraphId,
                                         exportStoreProperties,
                                         storeStoreProperties,
                                         exportSchema,
                                         storeSchema);
        } else if (graphLibrary.exists(exportGraphId)) {
            rtn = getGraphWithLibraryAndID(exportGraphId, graphLibrary);
        } else {
            rtn = getGraphAfterResolvingSchemaAndProperties(exportGraphId,
                                                            exportSchema,
                                                            exportStoreProperties,
                                                            exportParentSchemaIds,
                                                            exportParentStorePropertiesId,
                                                            graphLibrary,
                                                            storeStoreProperties,
                                                            storeSchema);
        }
        return rtn;
    }

    private Graph getGraphAfterResolvingSchemaAndProperties(final String exportGraphId, final Schema exportSchema, final StoreProperties exportStoreProperties, final List<String> exportParentSchemaIds, final String exportParentStorePropertiesId, final GraphLibrary graphLibrary, final StoreProperties storeStoreProperties, final Schema storeSchema) {
        final Graph rtn;
        StoreProperties storeProperties = resolveStoreProperties(exportStoreProperties,
                                                                 exportParentStorePropertiesId,
                                                                 graphLibrary,
                                                                 storeStoreProperties);

        Schema schema = resolveSchema(exportSchema,
                                      exportParentSchemaIds,
                                      graphLibrary,
                                      storeSchema);

        rtn = new Builder()
                .graphId(exportGraphId)
                .library(graphLibrary)
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
        return rtn;
    }

    private StoreProperties resolveStoreProperties(final StoreProperties exportStoreProperties, final String exportParentStorePropertiesId, final GraphLibrary graphLibrary, final StoreProperties storeStoreProperties) {
        StoreProperties storeProperties = null;
        if (exportParentStorePropertiesId != null) {
            storeProperties = graphLibrary.getProperties(exportParentStorePropertiesId);
        }
        if (exportStoreProperties != null) {
            if (storeProperties == null) {
                storeProperties = exportStoreProperties;
            } else {
                // delete the old properties id as we are about to modify the properties
                storeProperties.getProperties().remove(ID);
                storeProperties.getProperties().putAll(exportStoreProperties.getProperties());
            }
        }
        if (storeProperties == null) {
            storeProperties = storeStoreProperties;
        }
        return storeProperties;
    }

    private Schema resolveSchema(final Schema exportSchema, final List<String> exportParentSchemaIds, final GraphLibrary graphLibrary, final Schema storeSchema) {
        Schema schema = null;
        if (exportParentSchemaIds != null) {
            if (exportParentSchemaIds.size() == 1) {
                schema = graphLibrary.getSchema(exportParentSchemaIds.get(0));
            } else {
                final Schema.Builder schemaBuilder = new Schema.Builder();
                for (final String id : exportParentSchemaIds) {
                    schemaBuilder.merge(graphLibrary.getSchema(id));
                }
                schema = schemaBuilder.build();
            }
        }

        if (exportSchema != null) {
            if (schema == null) {
                schema = exportSchema;
            } else {
                // delete the old schema id as we are about to modify the schema
                schema = new Schema.Builder()
                        .merge(schema)
                        .id(null)
                        .merge(exportSchema)
                        .build();
            }
        }
        if (null == schema) {
            schema = storeSchema;
        }
        return schema;
    }

    private Graph getGraphWithLibraryAndID(final String exportGraphId, final GraphLibrary graphLibrary) {
        // If the graphId exists in the graphLibrary then just use it
        return new Graph.Builder()
                .graphId(exportGraphId)
                .library(graphLibrary)
                .build();
    }

    private Graph getGraphWithoutLibrary(final String exportGraphId, final StoreProperties exportStoreProperties, final StoreProperties storeStoreProperties, final Schema exportSchema, final Schema storeSchema) {
        // No store graph library so we create a new Graph
        final Schema schema = (null == exportSchema) ? storeSchema : exportSchema;
        final StoreProperties properties = (null == exportStoreProperties) ? storeStoreProperties : exportStoreProperties;
        return new Builder()
                .graphId(exportGraphId)
                .addSchema(schema)
                .storeProperties(properties)
                .build();
    }

    private void validationHandling(final Store store, final ExportToOtherGraph<?> export) {

        final ValidationResult validationResult = validate(export.getGraphId(),
                                                           export.getParentSchemaIds(),
                                                           export.getParentStorePropertiesId(),
                                                           store.getGraphLibrary(),
                                                           export.getSchema(),
                                                           export.getStoreProperties(),
                                                           store);

        if (!validationResult.isValid()) {
            throw new IllegalArgumentException(validationResult.getErrorString());
        }
    }

    public ValidationResult validate(final String exportGraphId,
                                     final List<String> exportParentSchemaIds,
                                     final String exportParentStorePropertiesId,
                                     final GraphLibrary graphLibrary,
                                     final Schema exportSchema,
                                     final StoreProperties exportStoreProperties,
                                     final Store store) {

        final ValidationResult result = new ValidationResult();

        if (store.getGraphId().equals(exportGraphId)) {
            result.addError("Cannot export to the same graph: " + exportGraphId);
        }
        if (null == graphLibrary) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != exportParentSchemaIds) {
                result.addError("parentSchemaId cannot be used without a GraphLibrary");
            }
            if (null != exportParentStorePropertiesId) {
                result.addError("parentStorePropertiesId cannot be used without a GraphLibrary");
            }
        } else if (graphLibrary.exists(exportGraphId)) {
            if (null != exportParentSchemaIds) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot use a different schema. Do not set the parentSchemaIds field");
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
            if (null != exportParentSchemaIds) {
                for (final String exportParentSchemaId : exportParentSchemaIds) {
                    if (null == store.getGraphLibrary().getSchema(exportParentSchemaId)) {
                        result.addError("Schema could not be found in the graphLibrary with id: " + exportParentSchemaIds);
                    }
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
