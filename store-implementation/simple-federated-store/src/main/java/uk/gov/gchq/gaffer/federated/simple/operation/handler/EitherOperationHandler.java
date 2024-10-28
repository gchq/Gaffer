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

package uk.gov.gchq.gaffer.federated.simple.operation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddToCacheHandler;

/**
 * Custom handler for operations that could in theory target sub graphs or the
 * federated store directly. Implements the {@link AddToCacheHandler} interface
 * so that this can also handle operations that add named operations etc.
 */
public class EitherOperationHandler<O extends Operation> implements AddToCacheHandler<O> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EitherOperationHandler.class);

    private final OperationHandler<O> standardHandler;

    public EitherOperationHandler(final OperationHandler<O> standardHandler) {
        this.standardHandler = standardHandler;
    }

    /**
     * If graph IDs are in the options the operation will be handled by a
     * {@link FederatedOperationHandler}, otherwise the original handler will be
     * used e.g. executed on the federated store directly.
     */
    @Override
    public Object doOperation(final O operation, final Context context, final Store store) throws OperationException {
        LOGGER.debug("Checking if Operation should be handled locally or on sub graphs: {}", operation);

        // If we have graph IDs then run as a federated operation
        if (operation.containsOption(FederatedOperationHandler.OPT_GRAPH_IDS) ||
                operation.containsOption(FederatedOperationHandler.OPT_SHORT_GRAPH_IDS) ||
                operation.containsOption(FederatedOperationHandler.OPT_EXCLUDE_GRAPH_IDS) ||
                operation.containsOption(FederatedOperationHandler.OPT_USE_DFLT_GRAPH_IDS)) {
            LOGGER.debug("Operation has specified graph IDs, it will be handled by sub graphs");
            return new FederatedOperationHandler<>().doOperation(operation, context, store);
        }

        // No sub graphs involved just run the handler for this operations on the federated store
        return standardHandler.doOperation(operation, context, store);
    }

    /**
     * We might be handling an Operation that extends {@link AddToCacheHandler}
     * so use the default handler for the suffix.
     */
    @Override
    public String getSuffixCacheName() {
        return ((AddToCacheHandler<O>) standardHandler).getSuffixCacheName();
    }
}
