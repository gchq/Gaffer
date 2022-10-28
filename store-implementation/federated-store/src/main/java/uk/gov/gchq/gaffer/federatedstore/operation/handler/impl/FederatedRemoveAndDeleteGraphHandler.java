/*
 * Copyright 2022 Crown Copyright
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

import org.apache.accumulo.core.client.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.DeleteAllDataOperation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.getConnector;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.isAccumulo;


/**
 * A handler for RemoveGraph operation for the FederatedStore.
 * And delete associated Accumulo Tables.
 * <p>
 *
 * @see FederatedStore
 * @see RemoveGraph
 */
public class FederatedRemoveAndDeleteGraphHandler extends FederatedRemoveGraphHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedRemoveAndDeleteGraphHandler.class);

    @Override
    public Boolean doOperation(final RemoveGraph operation, final Context context, final Store store) throws OperationException {
        try {
            //Get the graph before they are removed.
            final List<Graph> graphsToRemove = ((FederatedStore) store).getGraphs(context.getUser(), Collections.singletonList(operation.getGraphId()), operation);

            //Ask graphs to delete all data.
            for (final Graph graph : graphsToRemove) {
                if (graph.isSupported(DeleteAllDataOperation.class)) {
                    graph.execute(new DeleteAllDataOperation(), context);
                }
            }

            //Remove graphs from Federation
            final boolean removed = super.doOperation(operation, context, store);

            return removed;
        } catch (final Exception e) {
            throw new OperationException(String.format("Error deleting accumulo table: %s", operation.getGraphId()), e);
        }
    }
}
