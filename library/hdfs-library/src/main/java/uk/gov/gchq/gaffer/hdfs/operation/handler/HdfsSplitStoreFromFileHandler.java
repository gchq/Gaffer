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
package uk.gov.gchq.gaffer.hdfs.operation.handler;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.SplitStoreFromFileHandler;

import java.io.IOException;
import java.util.List;


public class HdfsSplitStoreFromFileHandler extends SplitStoreFromFileHandler {
    @Override
    public List<String> getSplits(final SplitStoreFromFile operation,
                                  final Context context,
                                  final Store store) throws OperationException {
        try {
            return super.getSplits(operation, context, store);
        } catch (final OperationException e) {
            // ignore error and try and load the splits from hdfs instead
        }

        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            return IOUtils.readLines(fs.open(new Path(operation.getInputPath())));
        } catch (final IOException e) {
            throw new OperationException("Failed to load splits from hdfs file: " + operation.getInputPath(), e);
        }
    }
}
