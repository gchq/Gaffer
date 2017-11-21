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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory.AccumuloAddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.tool.ImportElementsToAccumuloTool;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.tool.AddElementsFromHdfsTool;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.IOException;

public class AddElementsFromHdfsHandler implements OperationHandler<AddElementsFromHdfs> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromHdfsHandler.class);

    @Override
    public Void doOperation(final AddElementsFromHdfs operation,
                            final Context context, final Store store)
            throws OperationException {
        doOperation(operation, context, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final AddElementsFromHdfs operation,
                            final Context context, final AccumuloStore store)
            throws OperationException {
        validateOperation(operation);

        final String splitsFilePath = getPathWithSlashSuffix(operation.getWorkingPath()) + context.getJobId() + "/splits";
        LOGGER.info("Using working directory for splits files: " + splitsFilePath);
        operation.setSplitsFilePath(splitsFilePath);

        try {
            checkHdfsDirectories(operation, store);
        } catch (final IOException e) {
            throw new OperationException("Operation failed due to filesystem error: " + e.getMessage());
        }

        if (!operation.isUseProvidedSplits() && needsSplitting(store)) {
            sampleAndSplit(operation, context, store);
        }

        fetchElements(operation, store);
        final String skipImport = operation.getOption(AccumuloStoreConstants.ADD_ELEMENTS_FROM_HDFS_SKIP_IMPORT);
        if (null == skipImport || !"TRUE".equalsIgnoreCase(skipImport)) {
            importElements(operation, store);
        } else {
            LOGGER.info("Skipping import as {} was {}", AccumuloStoreConstants.ADD_ELEMENTS_FROM_HDFS_SKIP_IMPORT,
                    skipImport);
        }
    }

    private void validateOperation(final AddElementsFromHdfs operation) {
        if (null != operation.getMinMapTasks()) {
            LOGGER.warn("minMapTasks field will be ignored");
        }

        if (null != operation.getMaxMapTasks()) {
            LOGGER.warn("maxMapTasks field will be ignored");
        }

        if (null != operation.getNumReduceTasks() && (null != operation.getMinReduceTasks() || null != operation.getMaxReduceTasks())) {
            throw new IllegalArgumentException("minReduceTasks and/or maxReduceTasks should not be set if numReduceTasks is");
        }

        if (null != operation.getMinReduceTasks() && null != operation.getMaxReduceTasks()) {
            LOGGER.warn("Logic for the minimum may result in more reducers than the maximum set");
            if (operation.getMinReduceTasks() > operation.getMaxReduceTasks()) {
                throw new IllegalArgumentException("Minimum number of reducers must be less than the maximum number of reducers");
            }
        }

        if (null == operation.getSplitsFilePath()) {
            throw new IllegalArgumentException("splitsFilePath is required");
        }

        if (null == operation.getWorkingPath()) {
            throw new IllegalArgumentException("workingPath is required");
        }
    }

    /**
     * If there are less then 2 split points and more than 1 tablet server
     * we should calculate new splits.
     *
     * @param store the accumulo store
     * @return true if the table needs splitting
     * @throws OperationException if calculating the number of splits or the number of tablet servers fails.
     */
    private boolean needsSplitting(final AccumuloStore store) throws OperationException {
        boolean needsSplitting = false;

        final boolean lessThan2Splits;
        try {
            lessThan2Splits = store.getConnection().tableOperations().listSplits(store.getTableName(), 2).size() < 2;
        } catch (final TableNotFoundException | AccumuloSecurityException | StoreException | AccumuloException e) {
            throw new OperationException("Unable to get accumulo's split points", e);
        }

        if (lessThan2Splits) {
            final int numberTabletServers;
            try {
                numberTabletServers = store.getTabletServers().size();
            } catch (final StoreException e) {
                throw new OperationException("Unable to get accumulo's tablet servers", e);
            }

            if (numberTabletServers > 1) {
                needsSplitting = true;
            }
        }

        return needsSplitting;
    }

    private void sampleAndSplit(final AddElementsFromHdfs operation, final Context context, final AccumuloStore store) throws OperationException {
        LOGGER.info("Starting to sample input data to create splits points to set on the table");

        // Sample data for split points and split the table
        final String workingPath = operation.getWorkingPath();
        if (null == workingPath) {
            throw new IllegalArgumentException("Prior to adding the data, the table needs to be split. To do this the workingPath must be set to a temporary directory");
        }

        String tmpJobWorkingPath = getPathWithSlashSuffix(workingPath);
        tmpJobWorkingPath = tmpJobWorkingPath + context.getJobId();

        final String tmpSplitsOutputPath = tmpJobWorkingPath + "/sampleSplitsOutput";
        try {
            store.execute(new OperationChain.Builder()
                    .first(new SampleDataForSplitPoints.Builder()
                            .addInputMapperPairs(operation.getInputMapperPairs())
                            .jobInitialiser(operation.getJobInitialiser())
                            .mappers(operation.getNumMapTasks())
                            .validate(operation.isValidate())
                            .outputPath(tmpSplitsOutputPath)
                            .splitsFilePath(operation.getSplitsFilePath())
                            .options(operation.getOptions())
                            .build())
                    .then(new SplitStoreFromFile.Builder()
                            .inputPath(operation.getSplitsFilePath())
                            .options(operation.getOptions())
                            .build())
                    .build(), context);
        } finally {
            final FileSystem fs;
            try {
                fs = FileSystem.get(new JobConf(new Configuration()));
                final Path pathToDelete = new Path(tmpJobWorkingPath);
                if (fs.exists(pathToDelete)) {
                    fs.delete(pathToDelete, true);
                }
            } catch (final IOException e) {
                LOGGER.warn("Unable to delete temporary files used to calculate splits", e);
            }
        }
    }

    private String getPathWithSlashSuffix(final String path) {
        if (path.endsWith("/")) {
            return path;
        }
        return path + "/";
    }

    private void fetchElements(final AddElementsFromHdfs operation, final AccumuloStore store)
            throws OperationException {
        final AddElementsFromHdfsTool fetchTool = new AddElementsFromHdfsTool(new AccumuloAddElementsFromHdfsJobFactory(), operation, store);
        final int response;
        try {
            LOGGER.info("Running FetchElementsFromHdfsTool job");
            response = ToolRunner.run(fetchTool.getConfig(), fetchTool, new String[0]);
            LOGGER.info("Finished running FetchElementsFromHdfsTool job");
        } catch (final Exception e) {
            LOGGER.error("Failed to fetch elements from HDFS: {}", e.getMessage());
            throw new OperationException("Failed to fetch elements from HDFS", e);
        }

        if (AddElementsFromHdfsTool.SUCCESS_RESPONSE != response) {
            LOGGER.error("Failed to fetch elements from HDFS. Response code was {}", response);
            throw new OperationException("Failed to fetch elements from HDFS. Response code was: " + response);
        }
    }

    private void importElements(final AddElementsFromHdfs operation, final AccumuloStore store)
            throws OperationException {
        final ImportElementsToAccumuloTool importTool;
        final int response;
        importTool = new ImportElementsToAccumuloTool(operation.getOutputPath(), operation.getFailurePath(), store);
        try {
            LOGGER.info("Running import job");
            response = ToolRunner.run(importTool, new String[0]);
            LOGGER.info("Finished running import job");
        } catch (final Exception e) {
            LOGGER.error("Failed to import elements into Accumulo: {}", e.getMessage());
            throw new OperationException("Failed to import elements into Accumulo", e);
        }

        if (ImportElementsToAccumuloTool.SUCCESS_RESPONSE != response) {
            LOGGER.error("Failed to import elements into Accumulo. Response code was {}", response);
            throw new OperationException("Failed to import elements into Accumulo. Response code was: " + response);
        }
    }

    private void checkHdfsDirectories(final AddElementsFromHdfs operation, final AccumuloStore store) throws IOException {
        final AddElementsFromHdfsTool tool = new AddElementsFromHdfsTool(new AccumuloAddElementsFromHdfsJobFactory(), operation, store);

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
