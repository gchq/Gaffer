/*
 * Copyright 2023 Crown Copyright
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
package uk.gov.gchq.gaffer.proxystore.operation.handler;


import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Set;

public class ProxyGetTraitsHandler implements OutputOperationHandler<GetTraits, Set<StoreTrait>> {

    @Override
    public Set<StoreTrait> doOperation(final GetTraits operation, final Context context, final Store store) throws OperationException {
        if (store instanceof ProxyStore) {
            ProxyStore proxyStore = (ProxyStore) store;
            try {
                return proxyStore.fetchTraits();
            } catch (Exception e) {
                throw new OperationException(String.format("Error handling GetTraits from remote store due to:%s", e.getMessage()), e);
            }
        } else {
            throw new OperationException("Error this handler can only work with a ProxyStore");
        }
    }
}
