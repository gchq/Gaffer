/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.tool.SplitStoreTool;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.SortedSet;
import java.util.function.Consumer;

public class SplitStoreHandler implements OperationHandler<SplitStore> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitStoreHandler.class);

    @Override
    public Void doOperation(final SplitStore operation,
                            final Context context, final Store store) throws OperationException {
        splitStore(operation, ((AccumuloStore) store));
        return null;
    }

    private void splitStore(final SplitStore operation, final AccumuloStore store) throws OperationException {
        final SplitStoreTool splitTool = new SplitStoreTool(operation, new AccumuloSplitsConsumer(store));
        try {
            ToolRunner.run(splitTool, new String[0]);
        } catch (final Exception e) {
            throw new OperationException(e.getMessage(), e);
        }

        LOGGER.info("Completed splitting the store");
    }

    private static final class AccumuloSplitsConsumer implements Consumer<SortedSet<Text>> {
        private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSplitsConsumer.class);
        private final AccumuloStore store;

        private AccumuloSplitsConsumer(final AccumuloStore store) {
            this.store = store;
        }

        @Override
        public void accept(final SortedSet<Text> splits) {
            try {
                store.getConnection().tableOperations().addSplits(store.getTableName(), splits);
                LOGGER.info("Added {} splits to table {}", splits.size(), store.getTableName());
            } catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException | StoreException e) {
                LOGGER.error("Failed to add {} split points to table {}", splits.size(), store.getTableName());
                throw new RuntimeException("Failed to add split points to the table specified: " + e.getMessage(), e);
            }
        }
    }
}
