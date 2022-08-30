/*
 * Copyright 2017-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionIntersect;

import java.util.Set;

import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getDeprecatedGraphIds;

/**
 * returns a set of {@link StoreTrait} that are common for all visible graphs.
 * traits1 = [a,b,c]
 * traits2 = [b,c]
 * traits3 = [a,b]
 * return [b]
 */
public class FederatedGetTraitsHandler implements OutputOperationHandler<GetTraits, Set<StoreTrait>> {
    @Override
    public Set<StoreTrait> doOperation(final GetTraits operation, final Context context, final Store store) throws OperationException {
        try {
            return (Set<StoreTrait>) store.execute(
                    new FederatedOperation.Builder()
                            .op(operation)
                            .mergeFunction(new CollectionIntersect<Object>())
                            .graphIds(getDeprecatedGraphIds(operation)) // deprecate this line.
                            .build(), context);
        } catch (final Exception e) {
            throw new OperationException("Error getting federated traits.", e);
        }
    }
}
