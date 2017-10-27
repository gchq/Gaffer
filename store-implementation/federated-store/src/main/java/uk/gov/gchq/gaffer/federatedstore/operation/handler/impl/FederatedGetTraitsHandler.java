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
import uk.gov.gchq.gaffer.federatedstore.operation.GetTraits;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedGetTraitsHandler implements OutputOperationHandler<GetTraits, Iterable<? extends StoreTrait>> {

    @Override
    public Iterable<? extends StoreTrait> doOperation(final GetTraits operation, final Context context, final Store store) throws OperationException {
        String graphIdsCsv = operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS);

        FederatedStore federatedStore = (FederatedStore) store;
        try {
            return operation.getIsSupportedTraits()
                    ? federatedStore.getTraits()
                    : federatedStore.getCurrentTraits(context.getUser(), graphIdsCsv);
        } catch (final Exception e) {
            throw new OperationException("Error getting Traits. isSupportedTraits: " + operation.getIsSupportedTraits() + " graphIdsCsv: " + graphIdsCsv, e);
        }
    }
}
