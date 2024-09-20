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
    public static final String PROPERTIES = "storeProperties";
    public static final String OP_DECLARATIONS = "operationDeclarations";

    @Override
    public Map<String, Object> doOperation(final GetAllGraphInfo operation, final Context context, final Store store)
        throws OperationException {
            // Get all the graphs in the federated store
            Iterable<GraphSerialisable> graphList = ((FederatedStore) store).getAllGraphs();

            Map<String, Object> allGraphInfo = new HashMap<>();

            for (final GraphSerialisable gs : graphList) {
                // Get the various properties of the individual federated graphs
                Map<String, Object> graphInfo = new HashMap<>();
                graphInfo.put(DESCRIPTION, gs.getConfig().getDescription());
                graphInfo.put(HOOKS, gs.getConfig().getHooks());
                graphInfo.put(PROPERTIES, gs.getStoreProperties().getProperties());
                graphInfo.put(OP_DECLARATIONS, gs.getStoreProperties().getOperationDeclarations().getOperations());

                // Add the Graph ID and all properties associated with it
                allGraphInfo.put(gs.getConfig().getGraphId(), graphInfo);
            }

            return allGraphInfo;
        }
}
