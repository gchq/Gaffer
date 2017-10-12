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

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.List;

/**
 * A handler for GetAdjacentIds operation for the FederatedStore.
 *
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds
 */
public class FederatedGetAdjacentIdsHandler extends FederatedOperationOutputHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> {
    @Override
    protected CloseableIterable<? extends EntityId> mergeResults(final List<CloseableIterable<? extends EntityId>> results, final GetAdjacentIds operation, final Context context, final Store store) {
        // Concatenate all the results into 1 iterable
        if (results.isEmpty()) {
            throw new IllegalArgumentException(NO_RESULTS_TO_MERGE_ERROR);
        }
        return new ChainedIterable<>(results.toArray(new Iterable[results.size()]));
    }
}

