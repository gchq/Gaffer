/*
 * Copyright 2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.List;

/**
 * A generic handler for Operations with CloseableIterable of elements for FederatedStore.
 * Simply executes the operation on each delegate graph then chains the results together
 * using a {@link ChainedIterable}.
 *
 * @see FederatedOperationOutputHandler
 */
public class FederatedOperationIterableHandler<OP extends Output<O>, O extends Iterable> extends FederatedOperationOutputHandler<OP, O> {
    @Override
    protected O mergeResults(final List<O> results, final OP operation, final Context context, final Store store) {
        if (results.isEmpty()) {
            return (O) new EmptyClosableIterable<>();
        }

        // Concatenate all the results into 1 iterable
        return (O) new ChainedIterable<>(CollectionUtil.toIterableArray(results));
    }
}
