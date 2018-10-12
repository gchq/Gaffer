/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.utilities.WriteData;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.AggregateDataForGroup;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.CalculatePartitioner;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.SortFullGroup;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.serialisation.GraphPartitionerSerialiser;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.SparkParquetUtils;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Executes the process of importing a {@link JavaRDD} of {@link Element}s into a {@link ParquetStore}.
 */
public class AddElementsFromRDD {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromRDD.class);

    void addElementsFromRDD(final JavaRDD<Element> input, final Context context, final ParquetStore store) throws OperationException {
        final FileSystem fs = store.getFS();
        final ParquetStoreProperties storeProperties = store.getProperties();
        final SchemaUtils schemaUtils = store.getSchemaUtils();
        final SparkSession spark = SparkContextUtil.getSparkSession(context, store.getProperties());
        SparkParquetUtils.configureSparkForAddElements(spark, storeProperties);
        final String tempDir = store.getProperties().getTempFilesDir();

        // Locations for intermediate files
        final Function<String, String> groupToUnsortedUnaggregatedNewData =
                group -> tempDir + "/AddElementsFromRDDTemp/unsorted_unaggregated_new/group=" + group + "/";
        final Function<String, String> groupToUnsortedAggregatedNewData =
                group -> tempDir + "/AddElementsFromRDDTemp/unsorted_aggregated_new/group=" + group + "/";
        final Function<String, String> groupToSortedAggregatedNewData =
                group -> tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/group=" + group + "/";
        final Function<String, String> groupToSortedAggregatedNewDataReversed =
                group -> tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/reversed-group=" + group + "/";

        try {
            // Write data from input to unsorted files (for each partition of input, there will be one file per group)
            LOGGER.info("Writing data for input RDD");
            input.foreachPartition(new WriteData(groupToUnsortedUnaggregatedNewData, store.getSchema()));

            // Aggregate the new data and old data together (one job for each group)
            for (final String group : schemaUtils.getGroups()) {
                if (schemaUtils.getGafferSchema().getAggregatedGroups().contains(group)) {
                    LOGGER.info("Creating AggregateDataForGroup task for group {}", group);
                    final List<String> inputFiles = new ArrayList<>();
                    final String groupDirectoryNewData = groupToUnsortedUnaggregatedNewData.apply(group);
                    final FileStatus[] newData = fs
                            .listStatus(new Path(groupDirectoryNewData), path -> path.getName().endsWith(".parquet"));
                    Arrays.stream(newData).map(f -> f.getPath().toString()).forEach(inputFiles::add);
                    final List<Path> existingData = store.getFilesForGroup(group);
                    existingData.stream().map(p -> p.toString()).forEach(inputFiles::add);
                    final String outputDir = groupToUnsortedAggregatedNewData.apply(group);
                    LOGGER.info("AggregateDataForGroup task for group {} is being called ({} files as input, outputting to {})",
                            group, inputFiles.size(), outputDir);
                    new AggregateDataForGroup(fs, schemaUtils, group, inputFiles, outputDir, spark).call();
                } else {
                    LOGGER.info("Group {} does not require aggregation so not creating AggregateDataForGroup task", group);
                }
            }

            // Sort the data (one job for each group)
            for (final String group : schemaUtils.getGroups()) {
                final List<String> sortColumns = schemaUtils.columnsToSortBy(group, false);
                final List<String> inputFiles = new ArrayList<>();
                final String outputDir = groupToSortedAggregatedNewData.apply(group);
                if (schemaUtils.getGafferSchema().getAggregatedGroups().contains(group)) {
                    final String inputDir = groupToUnsortedAggregatedNewData.apply(group);
                    final FileStatus[] inputFilesFS = fs
                            .listStatus(new Path(inputDir), path -> path.getName().endsWith(".parquet"));
                    Arrays.stream(inputFilesFS).map(f -> f.getPath().toString()).forEach(inputFiles::add);
                } else {
                    // Input is new data from groupToUnsortedUnaggregatedNewData and old data from existing snapshot directory
                    final String groupDirectoryNewData = groupToUnsortedUnaggregatedNewData.apply(group);
                    final FileStatus[] newData = fs.listStatus(new Path(groupDirectoryNewData), path -> path.getName().endsWith(".parquet"));
                    Arrays.stream(newData).map(f -> f.getPath().toString()).forEach(inputFiles::add);
                    final List<Path> existingData = store.getFilesForGroup(group);
                    existingData.stream().map(p -> p.toString()).forEach(inputFiles::add);
                }
                LOGGER.info("SortFullGroup task for group {} is being called ({} files as input, outputting to {})",
                        group, inputFiles.size(), outputDir);
                new SortFullGroup(group,
                        schemaUtils.getEntityGroups().contains(group),
                        false,
                        schemaUtils,
                        sortColumns,
                        inputFiles,
                        outputDir,
                        store.getProperties().getAddElementsOutputFilesPerGroup(),
                        spark,
                        fs).call();
            }

            // For each edge group, sort by destination to create reversed edges files
            for (final String group : schemaUtils.getEdgeGroups()) {
                final List<String> sortColumns = schemaUtils.columnsToSortBy(group, true);
                final List<String> inputFiles = new ArrayList<>();
                final String inputDir = groupToSortedAggregatedNewData.apply(group);
                final FileStatus[] inputFilesFS = fs
                        .listStatus(new Path(inputDir), path -> path.getName().endsWith(".parquet"));
                Arrays.stream(inputFilesFS).map(f -> f.getPath().toString()).forEach(inputFiles::add);
                final String outputDir = groupToSortedAggregatedNewDataReversed.apply(group);
                new SortFullGroup(group,
                        schemaUtils.getEntityGroups().contains(group),
                        true,
                        schemaUtils,
                        sortColumns,
                        inputFiles,
                        outputDir,
                        store.getProperties().getAddElementsOutputFilesPerGroup(),
                        spark,
                        fs).call();
                LOGGER.info("SortFullGroup task for reversed edge group {} is being called ({} files as input, outputting to {})",
                        group, inputFiles.size(), outputDir);
            }

            // Create new graph partitioner
            LOGGER.info("Calculating new GraphPartitioner");
            final GraphPartitioner newPartitioner = new CalculatePartitioner(
                    new Path(tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/"), store.getSchema(), fs).call();
            LOGGER.info("New GraphPartitioner has partitions for {} groups, {} reversed edge groups",
                    newPartitioner.getGroups().size(), newPartitioner.getGroupsForReversedEdges().size());

            // Write out graph partitioner
            final Path newGraphPartitionerPath = new Path(tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/graphPartitioner");
            final FSDataOutputStream stream = fs.create(newGraphPartitionerPath);
            LOGGER.info("Writing graph partitioner to {}", newGraphPartitionerPath);
            new GraphPartitionerSerialiser().write(newPartitioner, stream);
            stream.close();

            // Move results to a new snapshot directory (the -tmp at the end allows us to add data to the directory,
            // and then when this is all finished we rename the directory to remove the -tmp; this allows us to make
            // the replacement of the old data with the new data an atomic operation and ensures that a get operation
            // against the store will not read the directory when only some of the data has been moved there).
            final long snapshot = System.currentTimeMillis();
            final String newDataDir = store.getDataDir() + "/" + ParquetStore.getSnapshotPath(snapshot) + "-tmp";
            LOGGER.info("Moving aggregated and sorted data to new snapshot directory {}", newDataDir);
            fs.mkdirs(new Path(newDataDir));
            fs.rename(new Path(tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/"),
                    new Path(newDataDir));

            // Move snapshot-tmp directory to snapshot
            final String directoryWithoutTmp = newDataDir.substring(0, newDataDir.lastIndexOf("-tmp"));
            LOGGER.info("Renaming {} to {}", newDataDir, directoryWithoutTmp);
            fs.rename(new Path(newDataDir), new Path(directoryWithoutTmp));

            // Set snapshot on store to new value
            LOGGER.info("Updating latest snapshot on store to {}", snapshot);
            store.setLatestSnapshot(snapshot);

            // Delete temporary data directory
            LOGGER.info("Deleting temporary directory {}", new Path(tempDir));
            fs.delete(new Path(tempDir), true);
        } catch (final IOException | StoreException e) {
            throw new OperationException("IOException adding elements from RDD", e);
        }
    }
}
