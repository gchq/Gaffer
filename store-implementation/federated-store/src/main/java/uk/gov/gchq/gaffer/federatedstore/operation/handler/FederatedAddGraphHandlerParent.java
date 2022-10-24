/*
 * Copyright 2018-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedNoOutputHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOutputIterableHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;

import static java.util.Objects.nonNull;

/**
 * A handler for operations that addGraph to the FederatedStore.
 *
 * @see OperationHandler
 * @see FederatedStore
 * @see GraphDelegate
 */
public abstract class FederatedAddGraphHandlerParent<OP extends AddGraph> implements OperationHandler<OP> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedAddGraphHandlerParent.class);
    public static final String ERROR_BUILDING_GRAPH_GRAPH_ID_S = "Error building graph %s";
    public static final String ERROR_ADDING_GRAPH_GRAPH_ID_S = "Error adding graph %s";
    public static final String USER_IS_LIMITED_TO_ONLY_USING_PARENT_PROPERTIES_ID_FROM_GRAPHLIBRARY_BUT_FOUND_STORE_PROPERTIES_S = "User is limited to only using parentPropertiesId from the graphLibrary, but found storeProperties: %s";

    @Override
    public Void doOperation(final OP operation, final Context context, final Store store) throws OperationException {
        final User user = context.getUser();
        final boolean isLimitedToLibraryProperties = ((FederatedStore) store).isLimitedToLibraryProperties(user, operation.isUserRequestingAdminUsage());

        if (isLimitedToLibraryProperties && nonNull(operation.getStoreProperties())) {
            throw new OperationException(String.format(USER_IS_LIMITED_TO_ONLY_USING_PARENT_PROPERTIES_ID_FROM_GRAPHLIBRARY_BUT_FOUND_STORE_PROPERTIES_S, operation.getProperties().toString()));
        }

        final GraphSerialisable graphSerialisable;
        try {
            graphSerialisable = _makeGraph(operation, store);
        } catch (final Exception e) {
            throw new OperationException(String.format(ERROR_BUILDING_GRAPH_GRAPH_ID_S, operation.getGraphId()), e);
        }

        try {
            //Add GraphSerialisable with Access values.
            ((FederatedStore) store).addGraphs(
                    operation.getGraphAuths(),
                    context.getUser().getUserId(),
                    operation.getIsPublic(),
                    operation.isDisabledByDefault(),
                    operation.getReadAccessPredicate(),
                    operation.getWriteAccessPredicate(),
                    graphSerialisable);
        } catch (final StorageException e) {
            throw new OperationException(e.getMessage(), e);
        } catch (final Exception e) {
            throw new OperationException(String.format(ERROR_ADDING_GRAPH_GRAPH_ID_S, operation.getGraphId()), e);
        }

        addGenericHandler((FederatedStore) store, graphSerialisable.getGraph());

        return null;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void addGenericHandler(final FederatedStore store, final Graph graph) {
        for (final Class<? extends Operation> supportedOperation : graph.getSupportedOperations()) {
            // some operations are not suitable for FederatedOperationGenericOutputHandler
            if (!store.isSupported(supportedOperation)) {
                if (Output.class.isAssignableFrom(supportedOperation)) {
                    final Class<? extends Output> supportedOutputOperation = (Class<? extends Output>) supportedOperation;

                    Class<?> outputClass;
                    try {
                        outputClass = supportedOutputOperation.newInstance().getOutputClass();
                    } catch (final InstantiationException |
                                   IllegalAccessException e) {
                        LOGGER.warn("Exception occurred while trying to create a newInstance of operation: {}", supportedOperation, e);
                        continue;
                    }
                    if (Iterable.class.isAssignableFrom(outputClass)) {
                        store.addOperationHandler((Class) supportedOutputOperation, new FederatedOutputIterableHandler(/*default merge*/));
                    } else {
                        LOGGER.warn("No generic default handler can be used for an Output operation that does not return CloseableIterable. operation: {}", supportedOutputOperation);
                    }
                } else {
                    store.addOperationHandler(supportedOperation, new FederatedNoOutputHandler());
                }
            }
        }
    }

    protected GraphSerialisable _makeGraph(final OP operation, final Store store) {
        return new GraphDelegate.Builder()
                .store(store)
                .graphId(operation.getGraphId())
                .schema(operation.getSchema())
                .storeProperties(operation.getStoreProperties())
                .parentSchemaIds(operation.getParentSchemaIds())
                .parentStorePropertiesId(operation.getParentPropertiesId())
                .buildGraphSerialisable();
    }
}
