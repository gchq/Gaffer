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
package uk.gov.gchq.gaffer.hdfs.operation.handler.job.tool;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

public class SplitStoreTool extends Configured implements Tool {
    public static final int SUCCESS_RESPONSE = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitStoreTool.class);

    private final SplitStore operation;
    private final Consumer<SortedSet<Text>> splitsConsumer;

    public SplitStoreTool(final SplitStore operation, final Consumer<SortedSet<Text>> splitsConsumer) {
        this.operation = operation;
        this.splitsConsumer = splitsConsumer;
    }

    @Override
    public int run(final String[] arg0) throws OperationException {
        LOGGER.info("Running SplitStoreTool");
        final Configuration conf = getConf();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
        } catch (final IOException e) {
            throw new OperationException("Failed to get Filesystem from configuration: " + e.getMessage(), e);
        }

        splitsConsumer.accept(readSplits(fs));

        return SUCCESS_RESPONSE;
    }

    private SortedSet<Text> readSplits(final FileSystem fs) throws OperationException {
        final SortedSet<Text> splits = new TreeSet<>();
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(new Path(operation.getInputPath())), CommonConstants.UTF_8))) {
            String line = br.readLine();
            while (null != line) {
                splits.add(new Text(Base64.decodeBase64(line)));
                line = br.readLine();
            }
        } catch (final IOException e) {
            throw new OperationException(e.getMessage(), e);
        }
        return Collections.unmodifiableSortedSet(splits);
    }
}
