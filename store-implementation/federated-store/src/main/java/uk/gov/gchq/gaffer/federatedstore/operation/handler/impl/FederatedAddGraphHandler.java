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
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;

/**
 * A handler for {@link AddGraph} operation for the FederatedStore.
 *
 * @see OperationHandler
 * @see FederatedStore
 * @see GraphDelegate
 */
public class FederatedAddGraphHandler implements OperationHandler<AddGraph> {

    public static final String ERROR_BUILDING_GRAPH_GRAPH_ID_S = "Error building graph %s";
    public static final String ERROR_ADDING_GRAPH_GRAPH_ID_S = "Error adding graph %s";
    public static final String USER_IS_LIMITED_TO_ONLY_USING_PARENT_PROPERTIES_ID_FROM_GRAPHLIBRARY_BUT_FOUND_STORE_PROPERTIES_S = "User is limited to only using parentPropertiesId from the graphLibrary, but found storeProperties: %s";

    @Override
    public Void doOperation(final AddGraph operation, final Context context, final Store store) throws OperationException {
        final User user = context.getUser();
        boolean isLimitedToLibraryProperties = ((FederatedStore) store).isLimitedToLibraryProperties(user);

        if (isLimitedToLibraryProperties && null != operation.getStoreProperties()) {
            throw new OperationException(String.format(USER_IS_LIMITED_TO_ONLY_USING_PARENT_PROPERTIES_ID_FROM_GRAPHLIBRARY_BUT_FOUND_STORE_PROPERTIES_S, operation.getProperties().toString()));
        }

        final Graph graph;
        try {
            graph = GraphDelegate.createGraph(store, operation.getGraphId(),
                    operation.getSchema(), operation.getStoreProperties(),
                    operation.getParentSchemaIds(), operation.getParentPropertiesId());
        } catch (final Exception e) {
            throw new OperationException(String.format(ERROR_BUILDING_GRAPH_GRAPH_ID_S, operation.getGraphId()), e);
        }

        try {
            ((FederatedStore) store).addGraphs(operation.getGraphAuths(), context.getUser().getUserId(), operation.getIsPublic(), graph);
        } catch (final StorageException e) {
            throw new OperationException(e.getMessage(), e);
        } catch (final Exception e) {
            throw new OperationException(String.format(ERROR_ADDING_GRAPH_GRAPH_ID_S, operation.getGraphId()), e);
        }
        return null;
    }
}
