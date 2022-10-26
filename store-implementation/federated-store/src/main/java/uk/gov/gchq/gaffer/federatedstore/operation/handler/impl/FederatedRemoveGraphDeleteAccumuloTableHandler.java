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
public class FederatedRemoveGraphDeleteAccumuloTableHandler extends FederatedRemoveGraphHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedRemoveGraphDeleteAccumuloTableHandler.class);

    @Override
    public Boolean doOperation(final RemoveGraph operation, final Context context, final Store store) throws OperationException {
        try {
            //Get the graph before they are removed.
            final List<Graph> graphsToRemove = ((FederatedStore) store).getGraphs(context.getUser(), Collections.singletonList(operation.getGraphId()), operation);

            //Check graphs align.
            if (operation.getDeleteTable() && operation.isUserRequestingAdminUsage()) {
                final Set<String> operationGraphIds = new HashSet<>(FederatedStoreUtil.getCleanStrings(operation.getGraphId()));
                final boolean mismatched = operationGraphIds.size() != graphsToRemove.size();
                if (mismatched) {
                    throwErrorForMismatch(graphsToRemove, operationGraphIds);
                }
            }


            //Remove graphs from Federation
            final boolean removed = super.doOperation(operation, context, store);

            if (removed) {
                if (operation.getDeleteTable() && !graphsToRemove.isEmpty()) {
                    deleteAccumuloTable(graphsToRemove);
                } else {
                    throw new OperationException("Error: Removing of graph returned true, but getGraphs was empty, no graphs exist to connect to accumulo and delete table.");
                }
            }

            return removed;
        } catch (final Exception e) {
            throw new OperationException(String.format("Error deleting accumulo table: %s", operation.getGraphId()), e);
        }
    }

    private void throwErrorForMismatch(final Collection<Graph> values, final Set<String> operationGraphIds) {
        final Set<String> remainder = values.stream()
                .map(Graph::getGraphId)
                .filter(s -> !operationGraphIds.contains(s))
                .collect(Collectors.toSet());

        /*
         * Current implementation of FederatedRemoveGraphHandler only takes 1 graphId, but if this changes then you have the problem
         * that changing the graphAccess in code here to allow table deletion is risky.
         * If 1 graphs fails, you'd need to recover the correct graphAccess.
         *
         * Alternative is FederatedStore.getGraphs() takes Admin request.
         */

        //TODO FS I think it does support Admin rights now.
        throw new UnsupportedOperationException("User is requesting to remove graphs and delete associated Accumulo tables with Admin rights," +
                " but the current implementation does not allow Admin rights to delete tables. As an Admin consider changing graphAccess and try again." +
                " graphsIds: " + remainder);
    }

    private static void deleteAccumuloTable(final List<Graph> graphsToRemove) throws GafferCheckedException {
        //currently only 1 graph is supported for Remove operation, but this is just future proofing.
        for (final Graph removeGraph : graphsToRemove) {
            //Update Tables
            if (isAccumulo(removeGraph)) {
                /*
                 * This logic is only for Accumulo derived stores Only.
                 * For updating table names to match graphs names.
                 *
                 * uk.gov.gchq.gaffer.accumulostore.[AccumuloStore, SingleUseAccumuloStore,
                 * MiniAccumuloStore, SingleUseMiniAccumuloStore]
                 */
                final String removeId = removeGraph.getGraphId();
                try {
                    Connector connection = getConnector((AccumuloProperties) removeGraph.getStoreProperties());
                    if (connection.tableOperations().exists(removeId)) {
                        connection.tableOperations().offline(removeId);
                        connection.tableOperations().delete(removeId);
                    }
                } catch (final Exception e) {
                    final String s = String.format("Error trying to drop tables for graphId:%s", removeId);
                    LOGGER.error(s, e);
                    throw new GafferCheckedException(s);
                }
            }
        }
    }
}
