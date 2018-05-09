/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
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
 * @see uk.gov.gchq.gaffer.operation.export.graph.handler.ExportToOtherGraphHandler
 */
public class GraphDelegate {

    public static final String SCHEMA_STRING = Schema.class.getSimpleName();
    public static final String STORE_PROPERTIES_STRING = StoreProperties.class.getSimpleName();
    public static final String SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S = "Schema could not be found in the graphLibrary with id: %s";
    public static final String GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S = "GraphId %s cannot be created without defined/known %s";
    public static final String STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S = "StoreProperties could not be found in the graphLibrary with id: %s";
    public static final String S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY = " %s cannot be used without a GraphLibrary";
    public static final String CANNOT_EXPORT_TO_THE_SAME_GRAPH_S = "Cannot export to the same Graph: %s";
    public static final String STORE_GRAPH_LIBRARY_IS_NULL = "Store GraphLibrary is null";
    public static final String USER_IS_NOT_AUTHORISED_TO_EXPORT_USING_S_S = "User is not authorised to export using %s: %s";
    public static final String GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD = "Graph: %s already exists so you cannot use a different %s. Do not set the %s field";
    public static final String GRAPH_LIBRARY_CANNOT_BE_FOUND_WITH_GRAPHID_S = "GraphLibrary cannot be found with graphId: %s";
    public static final String S_MUST_BE_SPECIFIED_WITH_S = "%s must be specified with %s";
    public static final String PARENT_SCHEMA_IDS = "parentSchemaIds";
    public static final String PARENT_STORE_PROPERTIES_ID = "parentStorePropertiesId";
    public static final String STORE_PROPERTIES_ID = "storePropertiesId";
    public static final String SCHEMA_ID = "schemaId";
    public static final String GRAPH_ID = "graphId";
    public static final String CANT_BOTH_BE_NULL = "%s and %s can't both be null";

    public static Graph createGraph(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        return new GraphDelegate().createGraphInstance(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, null);
    }

    public GraphSerialisable createGraphSerialisable(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        return createGraphSerialisable(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, null);
    }

    public Graph createGraphInstance(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        return createGraphInstance(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, null);
    }

    public GraphSerialisable createGraphSerialisable(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId, final GraphHook[] hooks) {
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final Pair<Schema, StoreProperties> existingGraphPair = null != graphLibrary ? graphLibrary.get(graphId) : null;

        validateGraph(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair);

        final Schema resolvedSchema = resolveSchemaForGraph(store, schema, parentSchemaIds, existingGraphPair);
        final StoreProperties resolvedStoreProperties = resolveStorePropertiesForGraph(store, storeProperties, parentStorePropertiesId, existingGraphPair);

        return new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(graphLibrary)
                        .addHooks(hooks)
                        .build())
                .schema(resolvedSchema)
                .properties(resolvedStoreProperties)
                .build();
    }

    public Graph createGraphInstance(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId, final GraphHook[] hooks) {
        return createGraphSerialisable(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, hooks).getGraph();
    }

    protected StoreProperties resolveStorePropertiesForGraph(final Store store, final StoreProperties properties, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {
        StoreProperties resultProps;
        if (null != existingGraphPair) {
            // If there is an existing graph then ignore any user provided properties and just use the existing properties
            resultProps = existingGraphPair.getSecond();
        } else {
            final GraphLibrary graphLibrary = store.getGraphLibrary();
            resultProps = (null == graphLibrary) ? properties : graphLibrary.resolveStoreProperties(properties, parentStorePropertiesId);
        }
        return resultProps;
    }

    protected Schema resolveSchemaForGraph(final Store store, final Schema schema, final List<String> parentSchemaIds, final Pair<Schema, StoreProperties> existingGraphPair) {
        Schema resultSchema;
        if (null != existingGraphPair) {
            // If there is an existing graph then ignore any user provided schemas and just use the existing schema
            resultSchema = existingGraphPair.getFirst();
        } else {
            final GraphLibrary graphLibrary = store.getGraphLibrary();
            resultSchema = (null == graphLibrary) ? schema : graphLibrary.resolveSchema(schema, parentSchemaIds);
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

    public void validateGraph(final Store store, final String graphId,
                              final Schema schema, final StoreProperties storeProperties,
                              final List<String> parentSchemaIds, final String parentStorePropertiesId) {
        final Pair<Schema, StoreProperties> existingGraphPair;
        if (null == store.getGraphLibrary()) {
            existingGraphPair = null;
        } else {
            existingGraphPair = store.getGraphLibrary().get(graphId);
        }

        validateGraph(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair);
    }

    public static void validate(final Store store, final String graphId,
                                final Schema schema, final StoreProperties storeProperties,
                                final List<String> parentSchemaIds, final String parentStorePropertiesId,
                                final Pair<Schema, StoreProperties> existingGraphPair) {
        ValidationResult result = validate(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair, new ValidationResult());
        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }

    public void validateGraph(final Store store, final String graphId,
                              final Schema schema, final StoreProperties storeProperties,
                              final List<String> parentSchemaIds, final String parentStorePropertiesId,
                              final Pair<Schema, StoreProperties> existingGraphPair) {
        ValidationResult result = validate(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair, new ValidationResult());
        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }

    protected static ValidationResult validate(final Store store, final String graphId,
                                               final Schema schema, final StoreProperties storeProperties,
                                               final List<String> parentSchemaIds, final String parentStorePropertiesId,
                                               final Pair<Schema, StoreProperties> existingGraphPair, final ValidationResult result) {
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        if (null == graphLibrary) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != parentSchemaIds) {
                result.addError(String.format(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, PARENT_SCHEMA_IDS));
            } else if (null == schema) {
                result.addError(String.format(CANT_BOTH_BE_NULL, SCHEMA_STRING, PARENT_SCHEMA_IDS));
            }

            if (null != parentStorePropertiesId) {
                result.addError(String.format(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, PARENT_STORE_PROPERTIES_ID));
            } else if (null == storeProperties) {
                result.addError(String.format(CANT_BOTH_BE_NULL, STORE_PROPERTIES_STRING, PARENT_STORE_PROPERTIES_ID));
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
                    result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, SCHEMA_STRING, PARENT_SCHEMA_IDS));
                }
            }

            if (null != parentStorePropertiesId) {
                StoreProperties fromLibrary = existingGraphPair.getSecond();
                if (!fromLibrary.equals(graphLibrary.getProperties(parentStorePropertiesId))) {
                    result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, STORE_PROPERTIES_STRING, PARENT_STORE_PROPERTIES_ID));
                }
            }

            if (null != schema && !schema.toString().equals(existingGraphPair.getFirst().toString())) {
                result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, SCHEMA_STRING, SCHEMA_STRING));
            }

            if (null != storeProperties && !existingGraphPair.getSecond().equals(storeProperties)) {
                result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, STORE_PROPERTIES_STRING, STORE_PROPERTIES_STRING));
            }
        } else {
            if (null != parentSchemaIds) {
                for (final String exportParentSchemaId : parentSchemaIds) {
                    if (null == graphLibrary.getSchema(exportParentSchemaId)) {
                        result.addError(String.format(SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, parentSchemaIds));
                    }
                }
            } else if (null == schema) {
                result.addError(String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, graphId, SCHEMA_STRING));
            }

            if (null != parentStorePropertiesId) {
                if (null == graphLibrary.getProperties(parentStorePropertiesId)) {
                    result.addError(String.format(STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, parentStorePropertiesId));
                }
            } else if (null == storeProperties) {
                result.addError(String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, graphId, STORE_PROPERTIES_STRING));
            }
        }

        return result;
    }

    public abstract static class BaseBuilder<BUILDER extends BaseBuilder> {
        protected Store store;
        protected String graphId;
        protected Schema schema;
        protected StoreProperties storeProperties;
        protected List<String> parentSchemaIds;
        protected String parentStorePropertiesId;

        public BUILDER _self() {
            return (BUILDER) this;
        }

        public BUILDER store(final Store store) {
            this.store = store;
            return _self();
        }

        public BUILDER graphId(final String graphId) {
            this.graphId = graphId;
            return _self();
        }

        public BUILDER schema(final Schema schema) {
            this.schema = schema;
            return _self();
        }

        public BUILDER storeProperties(final StoreProperties storeProperties) {
            this.storeProperties = storeProperties;
            return _self();
        }

        public BUILDER parentSchemaIds(final List<String> parentSchemaIds) {
            this.parentSchemaIds = parentSchemaIds;
            return _self();
        }

        public BUILDER parentStorePropertiesId(final String parentStorePropertiesId) {
            this.parentStorePropertiesId = parentStorePropertiesId;
            return _self();
        }

        public Graph createGraph() {
            return new GraphDelegate().createGraphInstance(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId);
        }

        public GraphSerialisable createGraphSerialisable() {
            return new GraphDelegate().createGraphSerialisable(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId);
        }

        public GraphSerialisable buildGraphSerialisable() {
            return createGraphSerialisable();
        }

        public Graph build() {
            return createGraph();
        }
    }

    public static class Builder extends BaseBuilder<Builder> {
        private GraphHook[] hooks = null;

        public Builder hooks(final GraphHook[] hooks) {
            this.hooks = hooks;
            return _self();
        }

        @Override
        public Builder _self() {
            return this;
        }

        @Override
        public Graph createGraph() {
            return new GraphDelegate().createGraphInstance(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, hooks);
        }

        @Override
        public GraphSerialisable createGraphSerialisable() {
            return new GraphDelegate().createGraphSerialisable(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, hooks);
        }
    }

}
