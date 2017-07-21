/*
 * Copyright 2017 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherAuthorisedGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.export.ExportToHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExportToOtherAuthorisedGraphHandler extends ExportToHandler<ExportToOtherAuthorisedGraph, OtherGraphExporter> {

    private Map<String, List<String>> idAuths = new HashMap<>();

    public Map<String, List<String>> getIdAuths() {
        return idAuths;
    }

    public void setIdAuths(Map<String, List<String>> idAuths) {
        if (idAuths == null) {
            this.idAuths = new HashMap<>();
        } else {
            this.idAuths = idAuths;
        }
    }

    @Override
    protected Class<OtherGraphExporter> getExporterClass() {
        return OtherGraphExporter.class;
    }

    @Override
    protected OtherGraphExporter createExporter(final ExportToOtherAuthorisedGraph export, final Context context, final Store store) {
        return new OtherGraphExporter(context.getUser(), context.getJobId(), createGraph(export, context, store));
    }

    protected Graph createGraph(final ExportToOtherAuthorisedGraph export, final Context context, final Store store) {
        final String exportGraphId = export.getGraphId();
        final List<String> exportParentSchemaIds = export.getParentSchemaIds();
        final String exportParentStorePropertiesId = export.getParentStorePropertiesId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        ValidationResult validationResult = validate(exportGraphId, exportParentSchemaIds, exportParentStorePropertiesId, graphLibrary, store);
        if (!validationResult.isValid()) {
            throw new IllegalArgumentException(validationResult.getErrorString());
        }

        if (export.getGraphId() != null && exportParentSchemaIds == null && exportParentStorePropertiesId == null) {
            if (isAuthorised(context.getUser(), idAuths.get(exportGraphId))) {
                return buildGraph(graphLibrary, exportGraphId, null, null, store);
            } else {
                throw new IllegalArgumentException("User is not authorised to export using graph id: " + exportGraphId);
            }
        } else if (export.getGraphId() != null && export.getParentSchemaIds() != null && export.getParentStorePropertiesId() != null) {
            if (isAuthorised(context.getUser(), idAuths.get(exportGraphId))) {
                if (isAuthorised(context.getUser(), idAuths.get(exportParentSchemaIds))) {
                    if (isAuthorised(context.getUser(), idAuths.get(exportParentStorePropertiesId))) {
                        return buildGraph(graphLibrary, exportGraphId, exportParentSchemaIds, exportParentStorePropertiesId, store);
                    } else {
                        throw new IllegalArgumentException("User is not authorised to export using parentStorePropertiesId: " + exportParentStorePropertiesId);
                    }
                } else {
                    throw new IllegalArgumentException("User is not authorised to export using parent Schema id(s): " + exportParentSchemaIds);
                }
            } else {
                throw new IllegalArgumentException("User is not authorised to export using graph id: " + exportGraphId);
            }
        }
        throw new IllegalArgumentException("Export failed, sorry");
    }

    private boolean isAuthorised(User user, List<String> auths) {
        if (auths != null && !auths.isEmpty()) {
            for (String auth : auths) {
                if (user.getOpAuths().contains(auth)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Graph buildGraph(final GraphLibrary graphLibrary, final String graphId,
                             final List<String> parentSchemaIds, final String parentStorePropertiesId,
                             final Store store) {
        if (graphLibrary == null) {
            throw new IllegalArgumentException("graphLibrary is null");
        }
        if (graphLibrary.exists(graphId)) {
            return new Graph.Builder()
                    .graphId(graphId)
                    .library(graphLibrary)
                    .build();
        }
        StoreProperties storeProperties = null;
        if (parentStorePropertiesId != null) {
            storeProperties = graphLibrary.getProperties(parentStorePropertiesId);
        }
        if (storeProperties == null) {
            storeProperties = store.getProperties();
        }

        Schema schema = null;
        if (parentSchemaIds != null) {
            if (parentSchemaIds.size() == 1) {
                schema = graphLibrary.getSchema(parentSchemaIds.get(0));
            } else {
                final Schema.Builder schemaBuilder = new Schema.Builder();
                for (final String id : parentSchemaIds) {
                    schemaBuilder.merge(graphLibrary.getSchema(id));
                }
                schema = schemaBuilder.build();
            }
        }

        if (null == schema) {
            schema = store.getSchema();
        }

        return new Graph.Builder()
                .graphId(graphId)
                .library(graphLibrary)
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
    }

    public static ValidationResult validate(final String graphId,
                                            final List<String> parentSchemaIds,
                                            final String parentStorePropertiesId,
                                            final GraphLibrary graphLibrary,
                                            final Store store) {

        final ValidationResult result = new ValidationResult();

        if (store.getGraphId().equals(graphId)) {
            result.addError("Cannot export to the same graph: " + graphId);
        }
        if (null == graphLibrary) {
            // GraphLibrary is required as only a graphId, a parentStorePropertiesId or a parentSchemaId can be given
            result.addError("GraphLibrary is null");
        } else if (graphLibrary.exists(graphId)) {
            if (null != parentSchemaIds) {
                result.addError("GraphId " + graphId + " already exists so you cannot use a different schema. Do not set the parentSchemaIds field");
            }
            if (null != parentStorePropertiesId) {
                result.addError("GraphId " + graphId + " already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field");
            }
        } else {
            if (null != parentSchemaIds) {
                for (final String exportParentSchemaId : parentSchemaIds) {
                    if (null == graphLibrary.getSchema(exportParentSchemaId)) {
                        result.addError("Schema could not be found in the graphLibrary with id: " + parentSchemaIds);
                    }
                }
            }
            if (null != parentStorePropertiesId) {
                if (null == graphLibrary.getProperties(parentStorePropertiesId)) {
                    result.addError("Store properties could not be found in the graphLibrary with id: " + parentStorePropertiesId);
                }
            }
        }

        return result;
    }
}
