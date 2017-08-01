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
        final String exportParentSchemaId = export.getParentSchemaId();
        final String exportParentStorePropertiesId = export.getParentStorePropertiesId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final Graph graph;

        validate(exportGraphId, exportParentSchemaId, exportParentStorePropertiesId, graphLibrary, store);

        if (!isAuthorised(context.getUser(), idAuths.get(exportGraphId))) {
            throw new IllegalArgumentException("User is not authorised to export using graphId: " + exportGraphId);
        }

        if (exportParentSchemaId == null && exportParentStorePropertiesId == null) {
            graph = createGraphWithLibraryAndId(graphLibrary, exportGraphId);
        } else {
            graph = createGraphAfterResolvingParentSchemaAndProperties(context.getUser(), graphLibrary, exportGraphId, exportParentSchemaId, exportParentStorePropertiesId);
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
                .graphId(graphId)
                .library(graphLibrary)
                .build();
    }

    private Graph createGraphAfterResolvingParentSchemaAndProperties(final User user,
                                                                     final GraphLibrary graphLibrary,
                                                                     final String graphId,
                                                                     final String parentSchemaId,
                                                                     final String parentStorePropertiesId) {
        final Schema schema = resolveSchema(user, graphLibrary, parentSchemaId);
        final StoreProperties storeProperties = resolveStoreProperties(user, graphLibrary, parentStorePropertiesId);

        return new Graph.Builder()
                .graphId(graphId)
                .library(graphLibrary)
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
    }

    private Schema resolveSchema(final User user, final GraphLibrary graphLibrary, final String parentSchemaId) {
        if (isAuthorised(user, idAuths.get(parentSchemaId))) {
            Schema schema = null;
            if (parentSchemaId != null) {
                schema = graphLibrary.getSchema(parentSchemaId);
            }
            return schema;
        } else {
            throw new IllegalArgumentException("User is not authorised to export using parentSchemaId: " + parentSchemaId);
        }
    }

    private StoreProperties resolveStoreProperties(final User user, final GraphLibrary graphLibrary, final String parentStorePropertiesId) {
        if (isAuthorised(user, idAuths.get(parentStorePropertiesId))) {
            StoreProperties storeProperties = null;
            if (parentStorePropertiesId != null) {
                storeProperties = graphLibrary.getProperties(parentStorePropertiesId);
            }
            return storeProperties;
        } else {
            throw new IllegalArgumentException("User is not authorised to export using parentStorePropertiesId: " + parentStorePropertiesId);
        }
    }

    private static void validate(final String graphId,
                                 final String parentSchemaId,
                                 final String parentStorePropertiesId,
                                 final GraphLibrary graphLibrary,
                                 final Store store) {

        final ValidationResult result = new ValidationResult();

        if (store.getGraphId().equals(graphId)) {
            result.addError("Cannot export to the same Graph: " + graphId);
        }
        if (null == graphLibrary) {
            // GraphLibrary is required as only a graphId, a parentStorePropertiesId or a parentSchemaId can be given
            result.addError("Store GraphLibrary is null");
        }
        if (graphLibrary.exists(graphId)) {
            if (null != parentSchemaId) {
                result.addError("GraphId " + graphId + " already exists so you cannot use a different Schema. Do not set the parentSchemaId field");
            }
            if (null != parentStorePropertiesId) {
                result.addError("GraphId " + graphId + " already exists so you cannot use different Store Properties. Do not set the parentStorePropertiesId field");
            }
        } else if (!graphLibrary.exists(graphId) && parentSchemaId == null && parentStorePropertiesId == null) {
            result.addError("GraphLibrary cannot be found with graphId: " + graphId);
        }
        if (parentSchemaId != null && parentStorePropertiesId == null) {
            result.addError("parentStorePropertiesId must be specified with parentSchemaId");
        }
        if (parentSchemaId == null && parentStorePropertiesId != null) {
            result.addError("parentSchemaId must be specified with parentStorePropertiesId");
        }
        if (parentSchemaId != null && parentStorePropertiesId != null) {
            if (null == graphLibrary.getSchema(parentSchemaId)) {
                result.addError("Schema could not be found in the GraphLibrary with id: " + parentSchemaId);
            }
            if (null == graphLibrary.getProperties(parentStorePropertiesId)) {
                result.addError("Store Properties could not be found in the GraphLibrary with id: " + parentStorePropertiesId);
            }
        }

        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }
}
