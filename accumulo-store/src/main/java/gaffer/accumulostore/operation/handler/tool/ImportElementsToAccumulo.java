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

package gaffer.accumulostore.operation.handler.tool;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.IngestUtils;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.StoreException;

public class ImportElementsToAccumulo extends Configured implements Tool {
    public static final int SUCCESS_RESPONSE = 0;

    private final AddElementsFromHdfs operation;
    private final Connector connector;
    private final String table;

    public ImportElementsToAccumulo(final AddElementsFromHdfs operation, final AccumuloStore store)
            throws StoreException {
        this.operation = operation;
        this.connector = store.getConnection();
        this.table = store.getProperties().getTable();
    }

    @Override
    public int run(final String[] strings) throws Exception {
        // Hadoop configuration
        final Configuration conf = getConf();
        final FileSystem fs = FileSystem.get(conf);

        // Make the failure directory
        fs.mkdirs(operation.getFailurePath());
        fs.setPermission(operation.getFailurePath(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

        // Remove the _SUCCESS file to prevent warning in accumulo
        fs.delete(new Path(operation.getOutputPath().toString() + "/_SUCCESS"), false);

        // Set all permissions
        IngestUtils.setDirectoryPermsForAccumulo(fs, operation.getOutputPath());

        // Import the files
        connector.tableOperations().importDirectory(table, operation.getOutputPath().toString(),
                operation.getFailurePath().toString(), false);

        // Delete the temporary directories
        fs.delete(operation.getFailurePath(), true);

        return SUCCESS_RESPONSE;
    }
}
