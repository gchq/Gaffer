/*
 * Copyright 2020 Crown Copyright
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
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Map;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.isUserRequestingAdminUsage;

public class FederatedGetAllGraphInfoHandler implements OutputOperationHandler<GetAllGraphInfo, Map<String, Object>> {

    @Override
    public Map<String, Object> doOperation(final GetAllGraphInfo operation, final Context context, final Store store) throws OperationException {
        try {
            final boolean userRequestingAdminUsage = isUserRequestingAdminUsage(operation);
            final String graphIdsCsv = operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS);
            return ((FederatedStore) store).getAllGraphsAndAuths(context.getUser(), graphIdsCsv, userRequestingAdminUsage);
        } catch (final Exception e) {
            throw new OperationException("Error getting graph information.", e);
        }
    }
}
