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
import uk.gov.gchq.gaffer.graph.GraphConfig;
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

    public void setIdAuths(final Map<String, List<String>> idAuths) {
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
        return new OtherGraphExporter(context.getUser(), createGraph(export, context, store));
    }

    protected Graph createGraph(final ExportToOtherAuthorisedGraph export, final Context context, final Store store) {
        final String exportGraphId = export.getGraphId();
        final List<String> exportParentSchemaIds = export.getParentSchemaIds();
        final String exportParentStorePropertiesId = export.getParentStorePropertiesId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final Graph graph;

        validate(context.getUser(), exportGraphId, exportParentSchemaIds, exportParentStorePropertiesId, graphLibrary, store);

        if (exportParentSchemaIds == null && exportParentStorePropertiesId == null) {
            graph = createGraphWithLibraryAndId(graphLibrary, exportGraphId);
        } else {
            graph = createGraphAfterResolvingParentSchemaAndProperties(graphLibrary, exportGraphId, exportParentSchemaIds, exportParentStorePropertiesId);
        }
        return graph;
    }

    private boolean isAuthorised(final User user, final List<String> auths) {
        if (auths != null && !auths.isEmpty()) {
            for (final String auth : auths) {
                if (user.getOpAuths().contains(auth)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Graph createGraphWithLibraryAndId(final GraphLibrary graphLibrary, final String graphId) {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(graphLibrary)
                        .build())
                .build();
    }

    private Graph createGraphAfterResolvingParentSchemaAndProperties(final GraphLibrary graphLibrary,
                                                                     final String graphId,
                                                                     final List<String> parentSchemaIds,
                                                                     final String parentStorePropertiesId) {
        final Schema schema = resolveSchema(graphLibrary, parentSchemaIds);
        final StoreProperties storeProperties = resolveStoreProperties(graphLibrary, parentStorePropertiesId);

        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(graphLibrary)
                        .build())
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
    }

    private Schema resolveSchema(final GraphLibrary graphLibrary, final List<String> parentSchemaIds) {
        Schema schema = null;
        if (parentSchemaIds != null) {
            if (parentSchemaIds.size() == 1) {
                final String schemaModule = parentSchemaIds.get(0);
                if (null == schemaModule) {
                    throw new IllegalArgumentException("schema could not be found in the GraphLibrary with id: " + parentSchemaIds.get(0));
                }
                schema = graphLibrary.getSchema(schemaModule);
            } else {
                final Schema.Builder schemaBuilder = new Schema.Builder();
                for (final String id : parentSchemaIds) {
                    final Schema schemaModule = graphLibrary.getSchema(id);
                    if (null == schemaModule) {
                        throw new IllegalArgumentException("schema could not be found in the GraphLibrary with id: " + id);
                    }
                    schemaBuilder.merge(schemaModule);
                }
                schema = schemaBuilder.build();
            }
        }

        return schema;
    }

    private StoreProperties resolveStoreProperties(final GraphLibrary graphLibrary, final String parentStorePropertiesId) {
        StoreProperties storeProperties = null;
        if (null != parentStorePropertiesId) {
            storeProperties = graphLibrary.getProperties(parentStorePropertiesId);
        }

        if (null == storeProperties) {
            throw new IllegalArgumentException("storeProperties could not be found in the GraphLibrary with id: " + parentStorePropertiesId);
        }

        return storeProperties;
    }

    private void validate(final User user,
                          final String graphId,
                          final List<String> parentSchemaIds,
                          final String parentStorePropertiesId,
                          final GraphLibrary graphLibrary,
                          final Store store) {

        final ValidationResult result = new ValidationResult();

        if (!isAuthorised(user, idAuths.get(graphId))) {
            throw new IllegalArgumentException("User is not authorised to export using graphId: " + graphId);
        }

        if (null != parentSchemaIds) {
            for (final String parentSchemaId : parentSchemaIds) {
                if (!isAuthorised(user, idAuths.get(parentSchemaId))) {
                    throw new IllegalArgumentException("User is not authorised to export using schemaId: " + parentSchemaId);
                }
            }
        }

        if (null != parentStorePropertiesId) {
            if (!isAuthorised(user, idAuths.get(parentStorePropertiesId))) {
                throw new IllegalArgumentException("User is not authorised to export using storePropertiesId: " + parentStorePropertiesId);
            }
        }

        if (store.getGraphId().equals(graphId)) {
            result.addError("Cannot export to the same Graph: " + graphId);
        }

        if (null == graphLibrary) {
            // GraphLibrary is required as only a graphId, a parentStorePropertiesId or a parentSchemaId can be given
            result.addError("Store GraphLibrary is null");
        } else if (graphLibrary.exists(graphId)) {
            if (null != parentSchemaIds) {
                result.addError("GraphId " + graphId + " already exists so you cannot use a different Schema. Do not set the parentSchemaIds field");
            }
            if (null != parentStorePropertiesId) {
                result.addError("GraphId " + graphId + " already exists so you cannot use different Store Properties. Do not set the parentStorePropertiesId field");
            }
        } else if (!graphLibrary.exists(graphId) && parentSchemaIds == null && parentStorePropertiesId == null) {
            result.addError("GraphLibrary cannot be found with graphId: " + graphId);
        }

        if (parentSchemaIds != null && parentStorePropertiesId == null) {
            result.addError("parentStorePropertiesId must be specified with parentSchemaId");
        }

        if (parentSchemaIds == null && parentStorePropertiesId != null) {
            result.addError("parentSchemaId must be specified with parentStorePropertiesId");
        }

        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }
}
