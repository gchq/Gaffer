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
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromIterable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.SortedSet;
import java.util.TreeSet;

public class SplitStoreFromIterableHandler implements OperationHandler<SplitStoreFromIterable<String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitStoreFromIterableHandler.class);

    @Override
    public Void doOperation(final SplitStoreFromIterable<String> operation,
                            final Context context, final Store store) throws OperationException {
        doOperation(operation, ((AccumuloStore) store));
        return null;
    }

    private void doOperation(final SplitStoreFromIterable<String> operation, final AccumuloStore store) throws OperationException {
        if (null == operation.getInput()) {
            throw new OperationException("Operation input is required.");
        }

        final SortedSet<Text> splits = new TreeSet<>();
        for (final String split : operation.getInput()) {
            splits.add(new Text(Base64.decodeBase64(split)));
        }

        try {
            store.getConnection().tableOperations().addSplits(store.getTableName(), splits);
            LOGGER.info("Added {} splits to table {}", splits.size(), store.getTableName());
        } catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException | StoreException e) {
            LOGGER.error("Failed to add {} split points to table {}", splits.size(), store.getTableName());
            throw new RuntimeException("Failed to add split points: " + e.getMessage(), e);
        }
    }
}
