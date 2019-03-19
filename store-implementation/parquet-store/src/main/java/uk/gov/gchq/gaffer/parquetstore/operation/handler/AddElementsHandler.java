/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.AggregateAndSortData;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.CallableResult;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.WriteUnsortedData;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.Partition;
import uk.gov.gchq.gaffer.parquetstore.partitioner.serialisation.GraphPartitionerSerialiser;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.SparkParquetUtils;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

/**
 * An {@link OperationHandler} for the {@link AddElements} operation on the {@link ParquetStore}.
 */
public class AddElementsHandler implements OperationHandler<AddElements> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsHandler.class);

    @Override
    public Void doOperation(final AddElements operation,
                            final Context context,
                            final Store store) throws OperationException {
        addElements(operation, context, (ParquetStore) store);
        return null;
    }

    private void addElements(final AddElements addElementsOperation,
                             final Context context,
                             final ParquetStore store) throws OperationException {
        // Set up
        final FileSystem fs = store.getFS();
        final Schema schema = store.getSchema();
        final SchemaUtils schemaUtils = store.getSchemaUtils();
        final SparkSession spark = SparkContextUtil.getSparkSession(context, store.getProperties());
        final ExecutorService threadPool = createThreadPool(spark, store.getProperties());
        final GraphPartitioner currentGraphPartitioner = store.getGraphPartitioner();
        SparkParquetUtils.configureSparkForAddElements(spark, store.getProperties());

        // Write data from addElementsOperation split by group and partition (NB this uses the existing partitioner -
        // adding elements using this operation does not effect the partitions).
        final String tmpDirectory = store.getTempFilesDir();
        final BiFunction<String, Integer, String> directoryForGroupAndPartitionId = (group, partitionId) ->
                tmpDirectory
                        + "/unsorted_unaggregated_new"
                        + "/group=" + group
                        + "/partition=" + partitionId;
        final BiFunction<String, Integer, String> directoryForGroupAndPartitionIdForReversedEdges = (group, partitionId) ->
                tmpDirectory
                        + "/unsorted_unaggregated_new"
                        + "/reversed-group=" + group
                        + "/partition=" + partitionId;
        LOGGER.info("Calling WriteUnsortedData to add elements");
        LOGGER.trace("currentGraphPartitioner is {}", currentGraphPartitioner);
        new WriteUnsortedData(store, currentGraphPartitioner,
                directoryForGroupAndPartitionId, directoryForGroupAndPartitionIdForReversedEdges)
                .writeElements(addElementsOperation.getInput());

        // For every group and partition, aggregate the new data with the old data and then sort
        final BiFunction<String, Integer, String> directoryForSortedResultsForGroupAndPartitionId = (group, partitionId) ->
                tmpDirectory
                        + "/sorted_new_old_merged"
                        + "/group=" + group
                        + "/partition=" + partitionId;
        final BiFunction<String, Integer, String> directoryForSortedResultsForGroupAndPartitionIdForReversedEdges = (group, partitionId) ->
                tmpDirectory
                        + "/sorted_new_old_merged"
                        + "/REVERSED-group=" + group
                        + "/partition=" + partitionId;
        final List<Callable<CallableResult>> tasks = new ArrayList<>();
        for (final String group : schema.getGroups()) {
            final List<Partition> partitions = currentGraphPartitioner.getGroupPartitioner(group).getPartitions();
            for (final Partition partition : partitions) {
                final List<String> inputFiles = new ArrayList<>();
                // New data
                inputFiles.add(directoryForGroupAndPartitionId.apply(group, partition.getPartitionId()));
                // Old data
                inputFiles.add(store.getFile(group, partition));
                final String outputDir = directoryForSortedResultsForGroupAndPartitionId.apply(group, partition.getPartitionId());
                final AggregateAndSortData task = new AggregateAndSortData(schemaUtils, fs, inputFiles, outputDir,
                        group, group + "-" + partition.getPartitionId(), false, store.getProperties().getCompressionCodecName(), spark);
                tasks.add(task);
                LOGGER.info("Created AggregateAndSortData task for group {}, partition {}", group, partition.getPartitionId());
            }
        }
        for (final String group : schema.getEdgeGroups()) {
            final List<Partition> partitions = currentGraphPartitioner.getGroupPartitionerForReversedEdges(group).getPartitions();
            for (final Partition partition : partitions) {
                final List<String> inputFiles = new ArrayList<>();
                // New data
                inputFiles.add(directoryForGroupAndPartitionIdForReversedEdges.apply(group, partition.getPartitionId()));
                // Old data
                inputFiles.add(store.getFileForReversedEdges(group, partition));
                final String outputDir = directoryForSortedResultsForGroupAndPartitionIdForReversedEdges.apply(group, partition.getPartitionId());
                final AggregateAndSortData task = new AggregateAndSortData(schemaUtils, fs, inputFiles, outputDir,
                        group, "reversed-" + group + "-" + partition.getPartitionId(), true, store.getProperties().getCompressionCodecName(), spark);
                tasks.add(task);
                LOGGER.info("Created AggregateAndSortData task for reversed edge group {}, partition {}", group, partition.getPartitionId());
            }
        }
        try {
            LOGGER.info("Invoking {} AggregateAndSortData tasks", tasks.size());
            final List<Future<CallableResult>> futures = threadPool.invokeAll(tasks);
            for (final Future<CallableResult> future : futures) {
                final CallableResult result = future.get();
                LOGGER.info("Result {} from task", result);
            }

        } catch (final InterruptedException e) {
            throw new OperationException("InterruptedException running AggregateAndSortData tasks", e);
        } catch (final ExecutionException e) {
            throw new OperationException("ExecutionException running AggregateAndSortData tasks", e);
        }

        try {
            // Move results to a new snapshot directory (the -tmp at the end allows us to add data to the directory,
            // and then when this is all finished we rename the directory to remove the -tmp; this allows us to make
            // the replacement of the old data with the new data an atomic operation and ensures that a get operation
            // against the store will not read the directory when only some of the data has been moved there).
            final long snapshot = System.currentTimeMillis();
            final String newDataDir = store.getDataDir() + "/" + ParquetStore.getSnapshotPath(snapshot) + "-tmp";
            LOGGER.info("Moving aggregated and sorted data to new snapshot directory {}", newDataDir);
            fs.mkdirs(new Path(newDataDir));
            for (final String group : schema.getGroups()) {
                final Path groupDir = new Path(newDataDir, ParquetStore.getGroupSubDir(group, false));
                fs.mkdirs(groupDir);
                LOGGER.info("Created directory {}", groupDir);
            }
            for (final String group : schema.getEdgeGroups()) {
                final Path groupDir = new Path(newDataDir, ParquetStore.getGroupSubDir(group, true));
                fs.mkdirs(groupDir);
                LOGGER.info("Created directory {}", groupDir);
            }
            for (final String group : schema.getGroups()) {
                final String groupDir = newDataDir + "/" + ParquetStore.getGroupSubDir(group, false);
                final List<Partition> partitions = currentGraphPartitioner.getGroupPartitioner(group).getPartitions();
                for (final Partition partition : partitions) {
                    final Path outputDir = new Path(directoryForSortedResultsForGroupAndPartitionId.apply(group, partition.getPartitionId()));
                    if (!fs.exists(outputDir)) {
                        LOGGER.info("Not moving data for group {}, partition id {} as the outputDir {} does not exist",
                                group, partition.getPartitionId(), outputDir);
                    } else {
                        // One .parquet file and one .parquet.crc file
                        final FileStatus[] status = fs.listStatus(outputDir, path -> path.getName().endsWith(".parquet"));
                        if (1 != status.length) {
                            LOGGER.error("Didn't find one Parquet file in path {} (found {} files)", outputDir, status.length);
                            throw new OperationException("Expected to find one Parquet file in path " + outputDir
                                    + " (found " + status.length + " files)");
                        } else {
                            final Path destination = new Path(groupDir, ParquetStore.getFile(partition.getPartitionId()));
                            LOGGER.info("Renaming {} to {}", status[0].getPath(), destination);
                            fs.rename(status[0].getPath(), destination);
                        }
                    }
                }
            }
            for (final String group : schema.getEdgeGroups()) {
                final String groupDir = newDataDir + "/" + ParquetStore.getGroupSubDir(group, true);
                final List<Partition> partitions = currentGraphPartitioner.getGroupPartitionerForReversedEdges(group).getPartitions();
                for (final Partition partition : partitions) {
                    final Path outputDir = new Path(directoryForSortedResultsForGroupAndPartitionIdForReversedEdges.apply(group, partition.getPartitionId()));
                    if (!fs.exists(outputDir)) {
                        LOGGER.info("Not moving data for reversed edge group {}, partition id {} as the outputDir {} does not exist",
                                group, partition.getPartitionId(), outputDir);
                    } else {
                        // One .parquet file and one .parquet.crc file
                        final FileStatus[] status = fs.listStatus(outputDir, path -> path.getName().endsWith(".parquet"));
                        if (1 != status.length) {
                            LOGGER.error("Didn't find one Parquet file in path {} (found {} files)", outputDir, status.length);
                            throw new OperationException("Expected to find one Parquet file in path " + outputDir
                                    + " (found " + status.length + " files)");
                        } else {
                            final Path destination = new Path(groupDir, ParquetStore.getFile(partition.getPartitionId()));
                            LOGGER.info("Renaming {} to {}", status[0].getPath(), destination);
                            fs.rename(status[0].getPath(), destination);
                        }
                    }
                }
            }

            // Delete temporary data directory
            LOGGER.info("Deleting temporary directory {}", tmpDirectory);
            fs.delete(new Path(tmpDirectory), true);
            // Write out graph partitioner (unchanged from previous one)
            final Path newGraphPartitionerPath = new Path(newDataDir + "/graphPartitioner");
            final FSDataOutputStream stream = fs.create(newGraphPartitionerPath);
            LOGGER.info("Writing graph partitioner to {}", newGraphPartitionerPath);
            new GraphPartitionerSerialiser().write(currentGraphPartitioner, stream);
            stream.close();
            // Move snapshot-tmp directory to snapshot
            final String directoryWithoutTmp = newDataDir.substring(0, newDataDir.lastIndexOf("-tmp"));
            LOGGER.info("Renaming {} to {}", newDataDir, directoryWithoutTmp);
            fs.rename(new Path(newDataDir), new Path(directoryWithoutTmp));
            // Set snapshot on store to new value
            LOGGER.info("Updating latest snapshot on store to {}", snapshot);
            store.setLatestSnapshot(snapshot);
        } catch (final IOException | StoreException e) {
            throw new OperationException("IOException moving results files into new snapshot directory", e);
        }
    }

    private static ExecutorService createThreadPool(final SparkSession spark, final ParquetStoreProperties storeProperties) {
        final int numberOfThreads;
        final Option<String> sparkDriverCores = spark.conf().getOption("spark.driver.cores");
        if (sparkDriverCores.nonEmpty()) {
            numberOfThreads = Integer.parseInt(sparkDriverCores.get());
        } else {
            numberOfThreads = storeProperties.getThreadsAvailable();
        }
        LOGGER.debug("Created thread pool of size {}", numberOfThreads);
        return Executors.newFixedThreadPool(numberOfThreads);
    }

}
