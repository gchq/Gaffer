/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation.handler.get;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple handler for getting information about the graphs contained in the federated store
 */
public class GetAllGraphInfoHandler implements OutputOperationHandler<GetAllGraphInfo, Map<String, Object>> {

    public static final String DESCRIPTION = "graphDescription";
    public static final String HOOKS = "graphHooks";
    public static final String STORE_CLASS = "storeClass";
    public static final String PROPERTIES = "storeProperties";
    public static final String OP_DECLARATIONS = "operationDeclarations";
    public static final String OWNER = "owner";
    public static final String IS_PUBLIC = "isPublic";

    @Override
    public Map<String, Object> doOperation(final GetAllGraphInfo operation, final Context context, final Store store)
        throws OperationException {
            Map<String, Object> allGraphInfo = new HashMap<>();

            // Get all the graphs in the federated store
            ((FederatedStore) store).getAllGraphsAndAccess().forEach(pair -> {
                Map<String, Object> graphInfo = new HashMap<>();
                GraphSerialisable graph = pair.getLeft();
                GraphAccess access = pair.getRight();

                // Get the various properties of the individual federated graphs
                graphInfo.put(DESCRIPTION, graph.getConfig().getDescription());
                graphInfo.put(HOOKS, graph.getConfig().getHooks());
                graphInfo.put(STORE_CLASS, graph.getStoreProperties().getStoreClass());
                graphInfo.put(OP_DECLARATIONS, graph.getStoreProperties().getOperationDeclarations().getOperations());
                // Get the access properties
                graphInfo.put(OWNER, access.getOwner());
                graphInfo.put(IS_PUBLIC, access.isPublic());

                // Only add the full properties if user has write access
                if (access.hasWriteAccess(context.getUser(), store.getProperties().getAdminAuth())) {
                    graphInfo.put(PROPERTIES, graph.getStoreProperties().getProperties());
                }

                // Add the Graph ID and all properties associated with it
                allGraphInfo.put(graph.getConfig().getGraphId(), graphInfo);
            });

            return allGraphInfo;
        }
}
