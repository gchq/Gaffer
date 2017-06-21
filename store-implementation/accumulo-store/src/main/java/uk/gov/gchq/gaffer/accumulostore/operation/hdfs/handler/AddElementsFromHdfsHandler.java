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
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.tool.FetchElementsFromHdfsTool;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.tool.ImportElementsToAccumuloTool;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.MapperGenerator;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
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

        if (null == operation.getSplitsFile()) {
            final String splitsFilePath = "/tmp/" + context.getJobId() + "/splits";
            LOGGER.info("Using temporary directory for splits files: " + splitsFilePath);
            operation.setSplitsFile(splitsFilePath);
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
            LOGGER.warn("minMaxTasks field will be ignored");
        }
    }

    private boolean needsSplitting(final AccumuloStore store) throws OperationException {
        boolean needsSplitting = false;

        final int numSplits;
        try {
            numSplits = store.getConnection().tableOperations().listSplits(store.getProperties().getTable(), Integer.MAX_VALUE).size();
        } catch (TableNotFoundException | AccumuloSecurityException | StoreException | AccumuloException e) {
            throw new OperationException("Unable to get accumulo's split points", e);
        }
        if (numSplits < 2) {
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
        final Class<? extends MapperGenerator> mapperGeneratorClass;
        try {
            mapperGeneratorClass = Class.forName(operation.getMapperGeneratorClassName()).asSubclass(MapperGenerator.class);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Mapper generator class name was invalid: " + operation.getMapperGeneratorClassName(), e);
        }

        final String outputPath = "/tmp/" + context.getJobId() + "/output";
        LOGGER.debug("Using temporary directory for split calculations: /tmp/" + context.getJobId());

        try {
            store._execute(new OperationChain.Builder()
                    .first(new SampleDataForSplitPoints.Builder()
                            .addInputPaths(operation.getInputPaths())
                            .jobInitialiser(operation.getJobInitialiser())
                            .mapperGenerator(mapperGeneratorClass)
                            .mappers(operation.getNumMapTasks())
                            .validate(operation.isValidate())
                            .outputPath(outputPath)
                            .resultingSplitsFilePath(operation.getSplitsFile())
                            .options(operation.getOptions())
                            .build())
                    .then(new SplitStore.Builder()
                            .inputPath(operation.getSplitsFile())
                            .options(operation.getOptions())
                            .build())
                    .build(), context);
        } finally {
            final FileSystem fs;
            try {
                fs = FileSystem.get(new JobConf(new Configuration()));
                final Path pathToDelete = new Path("/tmp/" + context.getJobId());
                if (fs.exists(pathToDelete)) {
                    fs.delete(pathToDelete, true);
                }
            } catch (final IOException e) {
                LOGGER.warn("Unable to delete temporary files used to calculate splits", e);
            }
        }
    }

    private void fetchElements(final AddElementsFromHdfs operation, final AccumuloStore store)
            throws OperationException {
        final FetchElementsFromHdfsTool fetchTool = new FetchElementsFromHdfsTool(operation, store);
        final int response;
        try {
            LOGGER.info("Running FetchElementsFromHdfsTool job");
            response = ToolRunner.run(fetchTool, new String[0]);
            LOGGER.info("Finished running FetchElementsFromHdfsTool job");
        } catch (final Exception e) {
            LOGGER.error("Failed to fetch elements from HDFS: {}", e.getMessage());
            throw new OperationException("Failed to fetch elements from HDFS", e);
        }

        if (FetchElementsFromHdfsTool.SUCCESS_RESPONSE != response) {
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
}
