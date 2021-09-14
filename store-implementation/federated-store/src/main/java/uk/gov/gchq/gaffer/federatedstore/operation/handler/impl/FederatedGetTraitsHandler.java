/*
 * Copyright 2017-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

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
            FederatedOperationChain<Void, StoreTrait> wrappedFedChain = new FederatedOperationChain.Builder<Void, StoreTrait>()
                    .operationChain(OperationChain.wrap(operation))
                    //deep copy options
                    .options(isNull(operation.getOptions()) ? new HashMap<>() : new HashMap<>(operation.getOptions()))
                    .build();

            final CloseableIterable<StoreTrait> concatResults = store.execute(wrappedFedChain, context);

            Map<StoreTrait, Integer> rtn;
            if (nonNull(concatResults) && nonNull(concatResults.iterator()) && concatResults.iterator().hasNext()) {
                rtn = Streams.toStream(concatResults)
                        // collect a map of k=trait v=count to covert concat of traits to an intersection of traits.
                        .collect(Collectors.toMap(t -> t, ignore -> 1, (existing, replacement) -> existing + replacement));

                long graphIdsSize = ((FederatedStore) store).getGraphs(context.getUser(), operation.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS), operation).stream().count();
                rtn.values().removeIf(v -> v < graphIdsSize);
            } else {
                rtn = Collections.EMPTY_MAP;
            }

            return rtn.keySet();
        } catch (final Exception e) {
            throw new OperationException("Error getting federated traits.", e);
        }
    }
}
