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
    public static final String S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY = " %s cannot be used without a GraphLibrary";
    public static final String CANNOT_EXPORT_TO_THE_SAME_GRAPH_S = "Cannot export to the same Graph: %s";
    public static final String GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD = "Graph: %s already exists so you cannot use a different %s. Do not set the %s field";

    private GraphDelegate() {
        // Private constructor to prevent instantiation.
    }

    public static Graph createGraph(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final Pair<Schema, StoreProperties> existingGraphPair = null != graphLibrary ? graphLibrary.get(graphId) : null;

        validate(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair);

        final Schema resolvedSchema = resolveSchema(store, schema, parentSchemaIds, existingGraphPair);
        final StoreProperties resolvedStoreProperties = resolveStoreProperties(store, storeProperties, parentStorePropertiesId, existingGraphPair);

        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(graphLibrary)
                        .build())
                .addSchema(resolvedSchema)
                .storeProperties(resolvedStoreProperties)
                .addToLibrary(false)
                .build();
    }

    private static StoreProperties resolveStoreProperties(final Store store, final StoreProperties properties, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {
        StoreProperties resultProps = null;
        if (null != existingGraphPair) {
            // If there is an existing graph then ignore any user provided properties and just use the existing properties
            resultProps = existingGraphPair.getSecond();
        } else {
            final GraphLibrary graphLibrary = store.getGraphLibrary();
            if (null != graphLibrary && null != parentStorePropertiesId) {
                resultProps = graphLibrary.getProperties(parentStorePropertiesId);
            }
            if (null != properties) {
                if (null == resultProps) {
                    resultProps = properties;
                } else {
                    resultProps.merge(properties);
                }
            }
            if (null == resultProps) {
                // If no properties have been provided then default to using the store properties
                resultProps = store.getProperties();
            }
        }
        return resultProps;
    }

    private static Schema resolveSchema(final Store store, final Schema schema, final List<String> parentSchemaIds, final Pair<Schema, StoreProperties> existingGraphPair) {
        Schema resultSchema = null;
        if (null != existingGraphPair) {
            // If there is an existing graph then ignore any user provided schemas and just use the existing schema
            resultSchema = existingGraphPair.getFirst();
        } else {
            final GraphLibrary graphLibrary = store.getGraphLibrary();
            if (null != graphLibrary && null != parentSchemaIds) {
                if (1 == parentSchemaIds.size()) {
                    resultSchema = graphLibrary.getSchema(parentSchemaIds.get(0));
                } else {
                    final Schema.Builder schemaBuilder = new Schema.Builder();
                    for (final String id : parentSchemaIds) {
                        schemaBuilder.merge(graphLibrary.getSchema(id));
                    }
                    resultSchema = schemaBuilder.build();
                }
            }
            if (null != schema) {
                if (null == resultSchema) {
                    resultSchema = schema;
                } else {
                    // delete the old schema id as we are about to modify the schema
                    resultSchema = new Schema.Builder()
                            .merge(resultSchema)
                            .merge(schema)
                            .build();
                }
            }
            if (null == resultSchema) {
                // If no schemas have been provided then default to using the store schema
                resultSchema = store.getSchema();
            }
        }
        return resultSchema;
    }

    public static void validate(final Store store, final String graphId,
                                final Schema schema, final StoreProperties storeProperties,
                                final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        final Pair<Schema, StoreProperties> existingGraphPair;
        if (null == store.getGraphLibrary()) {
            existingGraphPair = null;
        } else {
            existingGraphPair = store.getGraphLibrary().get(graphId);
        }

        validate(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair);
    }

    public static void validate(final Store store, final String graphId,
                                final Schema schema, final StoreProperties storeProperties,
                                final List<String> parentSchemaIds, final String parentStorePropertiesId,
                                final Pair<Schema, StoreProperties> existingGraphPair) {
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        final ValidationResult result = new ValidationResult();

        if (graphId.equals(store.getGraphId())) {
            result.addError(String.format(CANNOT_EXPORT_TO_THE_SAME_GRAPH_S, graphId));
        }
        if (null == graphLibrary) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != parentSchemaIds) {
                result.addError(String.format(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, "parentSchemaIds"));
            }
            if (null != parentStorePropertiesId) {
                result.addError(String.format(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, "parentStorePropertiesId"));
            }
        } else if (null != existingGraphPair) {

            if (null != parentSchemaIds) {
                Schema.Builder idFromLibrary = new Schema.Builder();
                for (final String parentSchemaId : parentSchemaIds) {
                    Schema tempSchema = graphLibrary.getSchema(parentSchemaId);
                    if (null != tempSchema) {
                        idFromLibrary.merge(tempSchema);
                    }
                }
                Schema fromLibrary = existingGraphPair.getFirst();
                if (!fromLibrary.toString().equals(idFromLibrary.build().toString())) {
                    result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, "Schema", "parentSchemaIds"));
                }
            }

            if (null != parentStorePropertiesId) {
                StoreProperties fromLibrary = existingGraphPair.getSecond();
                if (!fromLibrary.equals(graphLibrary.getProperties(parentStorePropertiesId))) {
                    result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, "StoreProperties", "parentStorePropertiesId"));
                }
            }

            if (null != schema && !schema.toString().equals(existingGraphPair.getFirst().toString())) {
                result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, "Schema", "schema"));
            }

            if (null != storeProperties && !existingGraphPair.getSecond().equals(storeProperties)) {
                result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, "StoreProperties", "storeProperties"));
            }
        } else {
            if (null != parentSchemaIds) {
                for (final String exportParentSchemaId : parentSchemaIds) {
                    if (null == graphLibrary.getSchema(exportParentSchemaId)) {
                        result.addError(String.format(SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, parentSchemaIds));
                    }
                }
            } else if (null == schema) {
                result.addError(String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, graphId, "Schema"));
            }

            if (null != parentStorePropertiesId) {
                if (null == graphLibrary.getProperties(parentStorePropertiesId)) {
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
