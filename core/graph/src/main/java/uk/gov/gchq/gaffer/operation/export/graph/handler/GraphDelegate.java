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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;

/**
 * Delegation class used to create a graph from the various combinations of
 * settings.
 * <ul>
 * <li>graphId
 * <li>storeProperties and/or parentPropertiesId</li>
 * <li>schema and/or parentSchemaIds</li>
 * </ul>
 *
 * @see ExportToOtherGraphHandler
 */
public final class GraphDelegate {

    public static final String SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S = "Schema could not be found in the graphLibrary with id: %s";
    public static final String GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S = "GraphId %s cannot be created without defined/known %s";
    public static final String STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S = "Store properties could not be found in the graphLibrary with id: %s";

    private GraphDelegate() {
        // Private constructor to prevent instantiation.
    }

    public static Graph createGraph(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        validate(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId);

        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final Graph rtn;

        if (null == graphLibrary) {
            rtn = createGraphWithoutLibrary(store, graphId, schema, storeProperties);
        } else if (graphLibrary.exists(graphId)) {
            rtn = createGraphWithLibraryAndId(graphId, graphLibrary);
        } else {
            rtn = createGraphAfterResolvingSchemaAndProperties(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId);
        }
        return rtn;
    }

    private static Graph createGraphAfterResolvingSchemaAndProperties(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        StoreProperties resolvedStoreProperties = resolveStoreProperties(store, storeProperties, parentStorePropertiesId);
        Schema resolvedSchema = resolveSchema(store, schema, parentSchemaIds);

        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(store.getGraphLibrary())
                        .build())
                .addSchema(resolvedSchema)
                .storeProperties(resolvedStoreProperties)
                .addToLibrary(false)
                .build();
    }

    private static StoreProperties resolveStoreProperties(final Store store, final StoreProperties properties, final String parentStorePropertiesId) {
        StoreProperties rtn = null;

        if (null != parentStorePropertiesId) {
            rtn = store.getGraphLibrary().getProperties(parentStorePropertiesId);
        }
        if (null != properties) {
            if (null == rtn) {
                rtn = properties;
            } else {
                // delete the old properties id as we are about to modify the properties
                rtn.getProperties().remove(StoreProperties.ID);
                rtn.getProperties().putAll(properties.getProperties());
            }
        }
        return rtn;
    }

    private static Schema resolveSchema(final Store store, final Schema schema, final List<String> parentSchemaIds) {
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        Schema rtn = null;
        if (null != parentSchemaIds) {
            if (1 == parentSchemaIds.size()) {
                rtn = graphLibrary.getSchema(parentSchemaIds.get(0));
            } else {
                final Schema.Builder schemaBuilder = new Schema.Builder();
                for (final String id : parentSchemaIds) {
                    schemaBuilder.merge(graphLibrary.getSchema(id));
                }
                rtn = schemaBuilder.build();
            }
        }

        if (null != schema) {
            if (null == rtn) {
                rtn = schema;
            } else {
                // delete the old schema id as we are about to modify the schema
                rtn = new Schema.Builder()
                        .merge(rtn)
                        .id(null)
                        .merge(schema)
                        .build();
            }
        }
        return rtn;
    }

    private static Graph createGraphWithLibraryAndId(final String graphId, final GraphLibrary graphLibrary) {
        // If the graphId exists in the graphLibrary then just use it
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(graphLibrary)
                        .build())
                .storeProperties(graphLibrary.get(graphId).getSecond())
                .addSchema(graphLibrary.get(graphId).getFirst())
                .addToLibrary(false)
                .build();
    }

    private static Graph createGraphWithoutLibrary(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties) {
        // No store graph library so we create a new Graph
        final Schema resolveSchema = (null == schema) ? store.getSchema() : schema;
        final StoreProperties resolveProperties = (null == storeProperties) ? store.getProperties() : storeProperties;

        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .build())
                .addSchema(resolveSchema)
                .storeProperties(resolveProperties)
                .addToLibrary(false)
                .build();
    }

    public static void validate(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        final ValidationResult result = new ValidationResult();

        if (graphId.equals(store.getGraphId())) {
            result.addError("Cannot export to the same graph: " + graphId);
        }
        if (null == graphLibrary) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != parentSchemaIds) {
                result.addError("parentSchemaIds cannot be used without a GraphLibrary");
            }
            if (null != parentStorePropertiesId) {
                result.addError("parentStorePropertiesId cannot be used without a GraphLibrary");
            }
        } else if (graphLibrary.exists(graphId)) {

            if (null != parentSchemaIds) {
                Schema.Builder idFromLibrary = new Schema.Builder();
                for (final String parentSchemaId : parentSchemaIds) {
                    Schema tempSchema = graphLibrary.getSchema(parentSchemaId);
                    if (null != tempSchema) {
                        idFromLibrary.merge(tempSchema);
                    }
                }
                Schema fromLibrary = graphLibrary.get(graphId).getFirst();
                if (!fromLibrary.toString().equals(idFromLibrary.build().toString())) {
                    result.addError("GraphId " + graphId + " already exists so you cannot use a different schema. Do not set the parentSchemaIds field.");
                }
            }

            if (null != parentStorePropertiesId) {
                StoreProperties fromLibrary = graphLibrary.get(graphId).getSecond();
                if (!fromLibrary.equals(graphLibrary.getProperties(parentStorePropertiesId))) {
                    result.addError("GraphId " + graphId + " already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field.");
                }
            }

            Pair<Schema, StoreProperties> schemaStorePropertiesPair = graphLibrary.get(graphId);
            if (null != schema && null != schemaStorePropertiesPair && !schema.toString().equals(schemaStorePropertiesPair.getFirst().toString())) {
                result.addError("GraphId " + graphId + " already exists so you cannot provide a different schema. Do not set the schema field.");
            }

            if (null != storeProperties && null != schemaStorePropertiesPair && !schemaStorePropertiesPair.getSecond().equals(storeProperties)) {
                result.addError("GraphId " + graphId + " already exists so you cannot provide different store properties. Do not set the storeProperties field.");
            }
        } else {
            if (null != parentSchemaIds) {
                for (final String exportParentSchemaId : parentSchemaIds) {
                    if (null == store.getGraphLibrary().getSchema(exportParentSchemaId)) {
                        result.addError(String.format(SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, parentSchemaIds));
                    }
                }
            } else if (null == schema) {
                result.addError(String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, graphId, "Schema"));
            }

            if (null != parentStorePropertiesId) {
                if (null == store.getGraphLibrary().getProperties(parentStorePropertiesId)) {
                    result.addError(String.format(STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, parentStorePropertiesId));
                }
            } else if (null == storeProperties) {
                result.addError(String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, graphId, "StoreProperties"));
            }
        }

        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }
}
