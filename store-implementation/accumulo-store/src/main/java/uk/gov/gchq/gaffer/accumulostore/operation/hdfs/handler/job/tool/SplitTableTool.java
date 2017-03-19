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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.tool;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.SplitTable;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.SortedSet;
import java.util.TreeSet;

public class SplitTableTool extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(SplitTableTool.class);
    public static final int SUCCESS_RESPONSE = 1;
    private AccumuloStore store;
    private SplitTable operation;

    public SplitTableTool(final SplitTable operation, final AccumuloStore store) {
        this.store = store;
        this.operation = operation;
    }

    @Override
    public int run(final String[] arg0) throws OperationException {
        LOGGER.info("Running SplitTableTool");
        final Configuration conf = getConf();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
        } catch (final IOException e) {
            throw new OperationException("Failed to get Filesystem from configuration: " + e.getMessage(), e);
        }
        final SortedSet<Text> splits = new TreeSet<>();
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(new Path(operation.getInputPath())), CommonConstants.UTF_8))) {
            String line = br.readLine();
            while (line != null) {
                splits.add(new Text(line));
                line = br.readLine();
            }
        } catch (final IOException e) {
            throw new OperationException(e.getMessage(), e);
        }

        try {
            store.getConnection().tableOperations().addSplits(store.getProperties().getTable(), splits);
            LOGGER.info("Added {} splits to table {}", splits.size(), store.getProperties().getTable());
        } catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException | StoreException e) {
            LOGGER.error("Failed to add {} split points to table {}", splits.size(), store.getProperties().getTable());
            throw new OperationException("Failed to add split points to the table specified: " + e.getMessage(), e);
        }

        return SUCCESS_RESPONSE;
    }

}
