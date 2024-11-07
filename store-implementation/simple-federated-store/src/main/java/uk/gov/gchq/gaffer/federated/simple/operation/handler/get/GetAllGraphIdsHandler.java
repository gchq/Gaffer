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

package uk.gov.gchq.gaffer.federated.simple.operation.handler.get;

import org.apache.commons.lang3.tuple.Pair;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GetAllGraphIdsHandler implements OutputOperationHandler<GetAllGraphIds, Set<String>> {

    @Override
    public Set<String> doOperation(final GetAllGraphIds operation, final Context context, final Store store) throws OperationException {
        // Get all the graphs and convert to a set of just IDs
        return StreamSupport.stream(((FederatedStore) store).getAllGraphsAndAccess().spliterator(), false)
            .map(Pair::getLeft)
            .map(GraphSerialisable::getGraphId)
            .collect(Collectors.toSet());
    }
}
