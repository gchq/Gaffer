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
import uk.gov.gchq.gaffer.federatedstore.operation.AddSchema;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class FederatedAddSchemaHandler implements OperationHandler<AddSchema> {

    public static final String ERROR_ADDING_SCHEMA_TO_FEDERATED_STORE_S = "Error adding schema to FederatedStore,%s";
    public static final String THE_STORE_DOESN_T_HAVE_A_GRAPH_LIBRARY = " the store doesn't have a graphLibrary";

    @Override
    public Void doOperation(final AddSchema operation, final Context context, final Store store) throws OperationException {
        FederatedStore federatedStore = (FederatedStore) store;
        GraphLibrary graphLibrary = federatedStore.getGraphLibrary();
        if (null == graphLibrary) {
            throw new OperationException(String.format(ERROR_ADDING_SCHEMA_TO_FEDERATED_STORE_S, THE_STORE_DOESN_T_HAVE_A_GRAPH_LIBRARY));
        } else {
            try {
                graphLibrary.addSchema(operation.getSchema());
            } catch (final Exception e) {
                throw new OperationException(String.format(ERROR_ADDING_SCHEMA_TO_FEDERATED_STORE_S, " schema: " + operation.getSchema()), e);
            }
        }
        return null;
    }

}
