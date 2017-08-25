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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;
import java.util.List;
import java.util.Properties;

/**
 * A handler for AddGraph operation for the FederatedStore.
 * To load a graph into the FederatedStore you need to provide three things.
 * <ul>
 * <li>GraphID
 * <li>Graph Schema
 * <li>Graph Properties file
 * </ul>
 *
 * @see OperationHandler
 * @see FederatedStore
 * @see AddGraph
 */
public class FederatedAddGraphHandler implements OperationHandler<AddGraph> {
    @Override
    public Void doOperation(final AddGraph operation, final Context context, final Store store) throws OperationException {
        final FederatedStore fedStore = (FederatedStore) store;
        fedStore.addGraphs(createGraph(operation, fedStore));
        return null;
    }

    protected Graph createGraph(final AddGraph operation, final Store store) {
        validate(operation, store);

        final String graphId = operation.getGraphId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();
        final Graph rtn;

        if (null == graphLibrary) {
            rtn = createGraphWithoutLibrary(operation, store);
        } else if (graphLibrary.exists(graphId)) {
            rtn = createGraphWithLibraryAndID(graphId, graphLibrary);
        } else {
            rtn = createGraphAfterResolvingSchemaAndProperties(operation, store);
        }
        return rtn;
    }

    private Graph createGraphAfterResolvingSchemaAndProperties(final AddGraph operation, final Store store) {
        StoreProperties storeProperties = resolveStoreProperties(operation, store);
        Schema schema = resolveSchema(operation, store);

        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(operation.getGraphId())
                        .library(store.getGraphLibrary())
                        .build())
                .addSchema(schema)
                .storeProperties(storeProperties)
                .build();
    }

    private StoreProperties resolveStoreProperties(final AddGraph operation, final Store store) {
        StoreProperties rtn = null;
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(operation.getProperties());
        final String parentStorePropertiesId = operation.getParentPropertiesId();

        if (parentStorePropertiesId != null) {
            rtn = store.getGraphLibrary().getProperties(parentStorePropertiesId);
        }
        if (storeProperties != null) {
            if (rtn == null) {
                rtn = storeProperties;
            } else {
                // delete the old properties id as we are about to modify the properties
                rtn.getProperties().remove(StoreProperties.ID);
                rtn.getProperties().putAll(storeProperties.getProperties());
            }
        }
        if (rtn == null) {
            rtn = store.getProperties();
        }
        return rtn;
    }

    private Schema resolveSchema(final AddGraph operation, final Store store) {
        final Schema schema = operation.getSchema();
        final List<String> parentSchemaIds = operation.getParentSchemaIds();
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        Schema rtn = null;
        if (parentSchemaIds != null) {
            if (parentSchemaIds.size() == 1) {
                rtn = graphLibrary.getSchema(parentSchemaIds.get(0));
            } else {
                final Schema.Builder schemaBuilder = new Schema.Builder();
                for (final String id : parentSchemaIds) {
                    schemaBuilder.merge(graphLibrary.getSchema(id));
                }
                rtn = schemaBuilder.build();
            }
        }

        if (schema != null) {
            if (rtn == null) {
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
                .storeProperties(graphLibrary.get(graphId).getSecond())
                .build();
    }

    private Graph createGraphWithoutLibrary(final AddGraph operation, final Store store) {
        // No store graph library so we create a new Graph
        Schema schema = operation.getSchema();
        schema = (null == schema) ? store.getSchema() : schema;
        Properties properties = operation.getProperties();
        properties = (null == properties) ? store.getProperties().getProperties() : properties;

        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(operation.getGraphId())
                        .build())
                .addSchema(schema)
                .storeProperties(StoreProperties.loadStoreProperties(properties))
                .build();
    }

    public void validate(final AddGraph operation, final Store store) {
        final String graphId = operation.getGraphId();
        final List<String> parentSchemaIds = operation.getParentSchemaIds();
        final String parentStorePropertiesId = operation.getParentPropertiesId();
        final GraphLibrary graphLibrary = store.getGraphLibrary();

        final ValidationResult result = new ValidationResult();

        if (graphId.equals(store.getGraphId())) {
            result.addError("Cannot add to itself: " + graphId);
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
                result.addError("GraphId " + graphId + " already exists so you cannot use a different schema. Do not set the parentSchemaIds field.");
            }
            if (null != operation.getSchema()) {
                result.addError("GraphId " + graphId + " already exists so you cannot provide a different schema. Do not set the schema field.");
            }
            if (null != parentStorePropertiesId) {
                result.addError("GraphId " + graphId + " already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field.");
            }
            if (null != operation.getProperties()) {
                result.addError("GraphId " + graphId + " already exists so you cannot provide different store properties. Do not set the storeProperties field.");
            }
        } else {
            if (null != parentSchemaIds) {
                for (final String exportParentSchemaId : parentSchemaIds) {
                    if (null == store.getGraphLibrary().getSchema(exportParentSchemaId)) {
                        result.addError("Schema could not be found in the graphLibrary with id: " + parentSchemaIds);
                    }
                }
            }
            if (null != parentStorePropertiesId) {
                if (null == store.getGraphLibrary().getProperties(parentStorePropertiesId)) {
                    result.addError("Store properties could not be found in the graphLibrary with id: " + parentStorePropertiesId);
                }
            }
        }

        if (!result.isValid()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
    }
}
