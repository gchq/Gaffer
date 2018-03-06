/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler.job.factory.HBaseAddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hbasestore.utils.IngestUtils;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.tool.AddElementsFromHdfsTool;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.IOException;

public class AddElementsFromHdfsHandler implements OperationHandler<AddElementsFromHdfs> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromHdfsHandler.class);

    @Override
    public Void doOperation(final AddElementsFromHdfs operation,
                            final Context context, final Store store)
            throws OperationException {
        doOperation(operation, (HBaseStore) store);
        return null;
    }

    public void doOperation(final AddElementsFromHdfs operation, final HBaseStore store) throws OperationException {
        validateOperation(operation);

        try {
            checkHdfsDirectories(operation, store);
        } catch (final IOException e) {
            throw new OperationException("Operation failed due to filesystem error: " + e.getMessage());
        }

        fetchElements(operation, store);
        final String skipImport = operation.getOption(HBaseStoreConstants.ADD_ELEMENTS_FROM_HDFS_SKIP_IMPORT);
        if (null == skipImport || !"TRUE".equalsIgnoreCase(skipImport)) {
            importElements(operation, store);
        } else {
            LOGGER.info("Skipping import as {} was {}", HBaseStoreConstants.ADD_ELEMENTS_FROM_HDFS_SKIP_IMPORT,
                    skipImport);
        }
    }

    private void validateOperation(final AddElementsFromHdfs operation) {
        if (null != operation.getMinMapTasks()) {
            LOGGER.warn("minMapTasks field will be ignored");
        }

        if (null != operation.getMaxMapTasks()) {
            LOGGER.warn("minMaxTasks field will be ignored");
        }

        if (null != operation.getMinReduceTasks()) {
            LOGGER.warn("minMapTasks field will be ignored");
        }

        if (null != operation.getMaxReduceTasks()) {
            LOGGER.warn("minMaxTasks field will be ignored");
        }
    }

    private void fetchElements(final AddElementsFromHdfs operation, final HBaseStore store)
            throws OperationException {
        final AddElementsFromHdfsTool fetchTool = new AddElementsFromHdfsTool(new HBaseAddElementsFromHdfsJobFactory(), operation, store);
        try {
            LOGGER.info("Running FetchElementsFromHdfsTool job");
            ToolRunner.run(fetchTool, new String[0]);
            LOGGER.info("Finished running FetchElementsFromHdfsTool job");
        } catch (final Exception e) {
            LOGGER.error("Failed to fetch elements from HDFS: {}", e.getMessage());
            throw new OperationException("Failed to fetch elements from HDFS", e);
        }
    }

    private void importElements(final AddElementsFromHdfs operation, final HBaseStore store)
            throws OperationException {
        final LoadIncrementalHFiles importTool;
        try {
            final Configuration conf = store.getConfiguration();
            conf.set(LoadIncrementalHFiles.CREATE_TABLE_CONF_KEY, "no");

            final FileSystem fs = FileSystem.get(conf);

            // Remove the _SUCCESS file to prevent warning in HBase
            LOGGER.info("Removing file {}/_SUCCESS", operation.getOutputPath());
            fs.delete(new Path(operation.getOutputPath() + "/_SUCCESS"), false);

            // Set all permissions
            IngestUtils.setDirectoryPermsForHbase(fs, new Path(operation.getOutputPath()));

            importTool = new LoadIncrementalHFiles(conf);
        } catch (final Exception e) {
            throw new OperationException("Failed to import elements into HBase", e);
        }

        try {
            LOGGER.info("Running import job");
            ToolRunner.run(importTool, new String[]{operation.getOutputPath(), store.getTableName().getNameAsString()});
            LOGGER.info("Finished running import job");
        } catch (final Exception e) {
            LOGGER.error("Failed to import elements into HBase: {}", e.getMessage());
            throw new OperationException("Failed to import elements into HBase", e);
        }
    }

    private void checkHdfsDirectories(final AddElementsFromHdfs operation, final HBaseStore store) throws IOException {
        final AddElementsFromHdfsTool tool = new AddElementsFromHdfsTool(new HBaseAddElementsFromHdfsJobFactory(), operation, store);

        LOGGER.info("Checking that the correct HDFS directories exist");
        final FileSystem fs = FileSystem.get(tool.getConfig());

        final Path outputPath = new Path(operation.getOutputPath());
        LOGGER.info("Ensuring output directory {} doesn't exist", outputPath);
        if (fs.exists(outputPath)) {
            if (fs.listFiles(outputPath, true).hasNext()) {
                LOGGER.error("Output directory exists and is not empty: {}", outputPath);
                throw new IllegalArgumentException("Output directory exists and is not empty: " + outputPath);
            }
            LOGGER.info("Output directory exists and is empty so deleting: {}", outputPath);
            fs.delete(outputPath, true);
        }
    }
}
