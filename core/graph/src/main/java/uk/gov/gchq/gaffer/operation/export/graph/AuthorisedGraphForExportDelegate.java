/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export.graph;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorisedGraphForExportDelegate extends GraphDelegate {

    private static Map<String, List<String>> idAuths = new HashMap<>();
    private static User user = new User();

    public Map<String, List<String>> getIdAuths() {
        return idAuths;
    }

    public void setIdAuths(final Map<String, List<String>> idAuths) {
        if (null == idAuths) {
            this.idAuths = new HashMap<>();
        } else {
            this.idAuths = idAuths;
        }
    }

    public User getUser() {
        return user;
    }

    public void setUser(final User user) {
        this.user = user;
    }

    public Graph createGraphInstance(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId, final Map idAuths, final User user) {
        setIdAuths(idAuths);
        setUser(user);

        return super.createGraphInstance(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId);
    }

    @Override
    protected Schema resolveSchemaForGraph(final Store store, final Schema schema, final List<String> parentSchemaIds, final Pair<Schema, StoreProperties> existingGraphPair) {
        Schema resultSchema = super.resolveSchemaForGraph(store, schema, parentSchemaIds, existingGraphPair);
        if (null == resultSchema) {
            // If no schemas have been provided then default to using the store schema
            resultSchema = store.getSchema();
        }
        return resultSchema;
    }

    @Override
    protected StoreProperties resolveStorePropertiesForGraph(final Store store, final StoreProperties properties, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {
        StoreProperties resultProps = super.resolveStorePropertiesForGraph(store, properties, parentStorePropertiesId, existingGraphPair);
        if (null == resultProps) {
            // If no properties have been provided then default to using the store properties
            resultProps = store.getProperties();
        }
        return resultProps;
    }

    @Override
    public void validateGraph(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {
        ValidationResult result = super.validate(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, existingGraphPair, new ValidationResult());

        Set<String> errors = result.getErrors();

        result.getErrors().removeIf(s -> s.equals(String.format(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, PARENT_SCHEMA_IDS))
                || s.equals(String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, graphId, SCHEMA_STRING))
                || s.equals(String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, graphId, STORE_PROPERTIES_STRING)));

        result = new ValidationResult();
        for (final String error : errors) {
            result.addError(error);
        }

        if (null == store.getGraphLibrary()) {
            // GraphLibrary is required as only a graphId, a parentStorePropertiesId or a parentSchemaId can be given
            result.addError(String.format(STORE_GRAPH_LIBRARY_IS_NULL));
        } else {
            // Check all the auths before anything else
            if (!isAuthorised(user, idAuths.get(graphId))) {
                result.addError(String.format(USER_IS_NOT_AUTHORISED_TO_EXPORT_USING_S_S, GRAPH_ID, graphId));
            }

            if (null != parentSchemaIds) {
                for (final String parentSchemaId : parentSchemaIds) {
                    if (!isAuthorised(user, idAuths.get(parentSchemaId))) {
                        result.addError(String.format(USER_IS_NOT_AUTHORISED_TO_EXPORT_USING_S_S, SCHEMA_ID, parentSchemaId));
                    }
                }
            }

            if (null != parentStorePropertiesId) {
                if (!isAuthorised(user, idAuths.get(parentStorePropertiesId))) {
                    result.addError(String.format(USER_IS_NOT_AUTHORISED_TO_EXPORT_USING_S_S, STORE_PROPERTIES_ID, parentStorePropertiesId));
                }
            }

            if (store.getGraphLibrary().exists(graphId)) {
                if (null != parentSchemaIds) {
                    result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, SCHEMA_STRING, PARENT_SCHEMA_IDS));
                }
                if (null != parentStorePropertiesId) {
                    result.addError(String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, graphId, STORE_PROPERTIES_STRING, PARENT_STORE_PROPERTIES_ID));
                }
            } else if (!store.getGraphLibrary().exists(graphId) && null == parentSchemaIds && null == parentStorePropertiesId) {
                result.addError(String.format(GRAPH_LIBRARY_CANNOT_BE_FOUND_WITH_GRAPHID_S, graphId));
            }

            if (null != parentSchemaIds && null == parentStorePropertiesId) {
                result.addError(String.format(S_MUST_BE_SPECIFIED_WITH_S, PARENT_STORE_PROPERTIES_ID, PARENT_SCHEMA_IDS));
            }

            if (null == parentSchemaIds && null != parentStorePropertiesId) {
                result.addError(String.format(S_MUST_BE_SPECIFIED_WITH_S, PARENT_SCHEMA_IDS, PARENT_STORE_PROPERTIES_ID));
            }

            if (graphId.equals(store.getGraphId())) {
                result.addError(String.format(CANNOT_EXPORT_TO_THE_SAME_GRAPH_S, graphId));
            }
        }

        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }

    private boolean isAuthorised(final User user, final List<String> auths) {
        if (null != auths && !auths.isEmpty()) {
            for (final String auth : auths) {
                if (user.getOpAuths().contains(auth)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static class Builder extends BaseBuilder<Builder> {
        private Map<String, List<String>> idAuths;
        private User user;

        public Builder idAuths(final Map<String, List<String>> idAuths) {
            this.idAuths = idAuths;
            return _self();
        }

        public Builder user(final User user) {
            this.user = user;
            return _self();
        }

        @Override
        public Builder _self() {
            return this;
        }

        @Override
        public Graph createGraph() {
            return new AuthorisedGraphForExportDelegate().createGraphInstance(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, this.idAuths, this.user);
        }
    }
}
