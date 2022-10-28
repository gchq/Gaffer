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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.apache.accumulo.core.client.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.DeleteAllDataOperation;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import static uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.getConnector;

public class DeleteAllDataHandler implements OperationHandler<DeleteAllDataOperation> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAllDataHandler.class);


    @Override
    public Object doOperation(final DeleteAllDataOperation operation, final Context context, final Store store) throws OperationException {
        try {
            //currently only 1 graph is supported for Remove operation, but this is just future proofing.
            //Update Tables
            final String removeId = store.getGraphId();
            try {
                Connector connection = getConnector((AccumuloProperties) store.getProperties());
                if (connection.tableOperations().exists(removeId)) {
                    connection.tableOperations().offline(removeId);
                    connection.tableOperations().delete(removeId);
                }
            } catch (final Exception e) {
                final String s = String.format("Error trying to drop tables for graphId:%s", removeId);
                LOGGER.error(s, e);
                throw new GafferCheckedException(s);
            }

            return null;
        } catch (final Exception e) {
            throw new OperationException(String.format("Error deleting accumulo table: %s", store.getGraphId()), e);
        }
    }
}
