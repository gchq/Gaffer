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
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphDelegate;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public Graph createGraph(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId, final Map idAuths, final User user) {
        setIdAuths(idAuths);
        setUser(user);
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

    @Override
    protected Schema resolveSchema(final Store store, final Schema schema, final List<String> parentSchemaIds, final Pair<Schema, StoreProperties> existingGraphPair) {
        Schema resultSchema = super.resolveSchema(store, schema, parentSchemaIds, existingGraphPair);
        if (null == resultSchema) {
            // If no schemas have been provided then default to using the store schema
            resultSchema = store.getSchema();
        }
        return resultSchema;
    }

    @Override
    protected StoreProperties resolveStoreProperties(final Store store, final StoreProperties properties, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {
        StoreProperties resultProps = super.resolveStoreProperties(store, properties, parentStorePropertiesId, existingGraphPair);
        if (null == resultProps) {
            // If no properties have been provided then default to using the store properties
            resultProps = store.getProperties();
        }
        return resultProps;
    }

    @Override
    protected void validate(final Store store, final String graphId, final Schema schema, final StoreProperties storeProperties, final List<String> parentSchemaIds, final String parentStorePropertiesId, final Pair<Schema, StoreProperties> existingGraphPair) {

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

        if (null == store.getGraphLibrary()) {
            // GraphLibrary is required as only a graphId, a parentStorePropertiesId or a parentSchemaId can be given
            result.addError("Store GraphLibrary is null");
        } else if (store.getGraphLibrary().exists(graphId)) {
            if (null != parentSchemaIds) {
                result.addError("GraphId " + graphId + " already exists so you cannot use a different Schema. Do not set the parentSchemaIds field");
            }
            if (null != parentStorePropertiesId) {
                result.addError("GraphId " + graphId + " already exists so you cannot use different Store Properties. Do not set the parentStorePropertiesId field");
            }
        } else if (!store.getGraphLibrary().exists(graphId) && null == parentSchemaIds && null == parentStorePropertiesId) {
            result.addError("GraphLibrary cannot be found with graphId: " + graphId);
        }

        if (null != parentSchemaIds && null == parentStorePropertiesId) {
            result.addError("parentStorePropertiesId must be specified with parentSchemaId");
        }

        if (null == parentSchemaIds && null != parentStorePropertiesId) {
            result.addError("parentSchemaId must be specified with parentStorePropertiesId");
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

    public static class Builder extends BaseBuilder<GraphForExportDelegate, Builder> {
        private Map<String, List<String>> idAuths;
        private User user;

        public Builder idAuths(final Map<String, List<String>> idAuths) {
            this.idAuths = idAuths;
            return this;
        }

        public Builder user(final User user) {
            this.user = user;
            return this;
        }

        @Override
        public Graph createGraph() {
            return new AuthorisedGraphForExportDelegate().createGraph(store, graphId, schema, storeProperties, parentSchemaIds, parentStorePropertiesId, this.idAuths, this.user);
        }
    }
}
