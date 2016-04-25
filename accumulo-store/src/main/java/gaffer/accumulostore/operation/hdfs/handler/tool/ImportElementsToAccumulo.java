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
package gaffer.accumulostore.operation.hdfs.handler.tool;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.IngestUtils;
import gaffer.accumulostore.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public class ImportElementsToAccumulo extends Configured implements Tool {
    public static final int SUCCESS_RESPONSE = 0;

    private final String inputPath;
    private final String failurePath;
    private final AccumuloStore store;

    public ImportElementsToAccumulo(final String inputPath, final String failurePath, final AccumuloStore store) {
        this.inputPath = inputPath;
        this.failurePath = failurePath;
        this.store = store;
    }

    @Override
    public int run(final String[] strings) throws Exception {
        TableUtils.ensureTableExists(store);

        // Hadoop configuration
        final Configuration conf = getConf();
        final FileSystem fs = FileSystem.get(conf);

        // Remove the _SUCCESS file to prevent warning in accumulo
        fs.delete(new Path(inputPath + "/_SUCCESS"), false);

        // Set all permissions
        IngestUtils.setDirectoryPermsForAccumulo(fs, new Path(inputPath));

        // Import the files
        store.getConnection().tableOperations().importDirectory(store.getProperties().getTable(), inputPath,
                failurePath, false);

        return SUCCESS_RESPONSE;
    }
}
