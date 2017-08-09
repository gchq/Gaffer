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
import uk.gov.gchq.gaffer.graph.GraphConfig;
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
    @Override
    protected Class<OtherGraphExporter> getExporterClass() {
        return OtherGraphExporter.class;
    }

    @Override
    protected OtherGraphExporter createExporter(final ExportToOtherGraph export, final Context context, final Store store) {
        return new OtherGraphExporter(context.getUser(), createGraph(export, store));
    }

    protected Graph createGraph(final ExportToOtherGraph export, final Store store) {
        validate(export, store);

        final String exportGraphId = export.getGraphId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final Graph rtn;

        if (null == graphLibrary) {
            rtn = createGraphWithoutLibrary(export, store);
        } else if (graphLibrary.exists(exportGraphId)) {
            rtn = createGraphWithLibraryAndID(exportGraphId, graphLibrary);
        } else {
            rtn = createGraphAfterResolvingSchemaAndProperties(export, store);
        }
        return rtn;
    }

    private Graph createGraphAfterResolvingSchemaAndProperties(final ExportToOtherGraph export, final Store store) {
        StoreProperties storeProperties = resolveStoreProperties(export, store);
        Schema schema = resolveSchema(export, store);

        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(export.getGraphId())
                        .library(store.getGraphLibrary())
                        .build())
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
    }

    private StoreProperties resolveStoreProperties(final ExportToOtherGraph export, final Store store) {
        StoreProperties rtn = null;
        final StoreProperties exportStoreProperties = export.getStoreProperties();
        final String exportParentStorePropertiesId = export.getParentStorePropertiesId();

        if (exportParentStorePropertiesId != null) {
            rtn = store.getGraphLibrary().getProperties(exportParentStorePropertiesId);
        }
        if (exportStoreProperties != null) {
            if (rtn == null) {
                rtn = exportStoreProperties;
            } else {
                // delete the old properties id as we are about to modify the properties
                rtn.getProperties().remove(StoreProperties.ID);
                rtn.getProperties().putAll(exportStoreProperties.getProperties());
            }
        }
        if (rtn == null) {
            rtn = store.getProperties();
        }
        return rtn;
    }

    private Schema resolveSchema(final ExportToOtherGraph export, final Store store) {
        final Schema exportSchema = export.getSchema();
        final List<String> exportParentSchemaIds = export.getParentSchemaIds();
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        Schema rtn = null;
        if (exportParentSchemaIds != null) {
            if (exportParentSchemaIds.size() == 1) {
                rtn = graphLibrary.getSchema(exportParentSchemaIds.get(0));
            } else {
                final Schema.Builder schemaBuilder = new Schema.Builder();
                for (final String id : exportParentSchemaIds) {
                    schemaBuilder.merge(graphLibrary.getSchema(id));
                }
                rtn = schemaBuilder.build();
            }
        }

        if (exportSchema != null) {
            if (rtn == null) {
                rtn = exportSchema;
            } else {
                // delete the old schema id as we are about to modify the schema
                rtn = new Schema.Builder()
                        .merge(rtn)
                        .id(null)
                        .merge(exportSchema)
                        .build();
            }
        }
        if (null == rtn) {
            rtn = store.getSchema();
        }
        return rtn;
    }

    private Graph createGraphWithLibraryAndID(final String graphId, final GraphLibrary graphLibrary) {
        // If the graphId exists in the graphLibrary then just use it
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(graphLibrary)
                        .build())
                .build();
    }

    private Graph createGraphWithoutLibrary(final ExportToOtherGraph export, final Store store) {
        // No store graph library so we create a new Graph
        final Schema exportSchema = export.getSchema();
        final Schema schema = (null == exportSchema) ? store.getSchema() : exportSchema;
        final StoreProperties exportStoreProperties = export.getStoreProperties();
        final StoreProperties properties = (null == exportStoreProperties) ? store.getProperties() : exportStoreProperties;
        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(export.getGraphId())
                        .build())
                .addSchema(schema)
                .storeProperties(properties)
                .build();
    }

    public void validate(final ExportToOtherGraph export, final Store store) {
        final String exportGraphId = export.getGraphId();
        final List<String> exportParentSchemaIds = export.getParentSchemaIds();
        final String exportParentStorePropertiesId = export.getParentStorePropertiesId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        final ValidationResult result = new ValidationResult();

        if (exportGraphId.equals(store.getGraphId())) {
            result.addError("Cannot export to the same graph: " + exportGraphId);
        }
        if (null == graphLibrary) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != exportParentSchemaIds) {
                result.addError("parentSchemaIds cannot be used without a GraphLibrary");
            }
            if (null != exportParentStorePropertiesId) {
                result.addError("parentStorePropertiesId cannot be used without a GraphLibrary");
            }
        } else if (graphLibrary.exists(exportGraphId)) {
            if (null != exportParentSchemaIds) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot use a different schema. Do not set the parentSchemaIds field.");
            }
            if (null != export.getSchema()) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot provide a different schema. Do not set the schema field.");
            }
            if (null != exportParentStorePropertiesId) {
                result.addError("GraphId " + exportGraphId + " already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field.");
            }
            if (null != export.getStoreProperties()) {
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

        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }
}
