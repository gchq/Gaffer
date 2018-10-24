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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
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
import uk.gov.gchq.gaffer.store.schema.Schema;

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

    private final ParquetStore store;
    private final Schema schema;
    private final SchemaUtils schemaUtils;
    private final FileSystem fs;
    private final SparkSession spark;
    private final String tempDir;
    private final Function<String, String> groupToUnsortedUnaggregatedNewData;
    private final Function<String, String> groupToUnsortedAggregatedNewData;
    private final Function<String, String> groupToSortedAggregatedNewData;
    private final Function<String, String> groupToSortedAggregatedNewDataReversed;

    public AddElementsFromRDD(final Context context, final ParquetStore store) {
        this.store = store;
        this.schema = store.getSchema();
        this.schemaUtils = store.getSchemaUtils();
        this.fs = store.getFS();
        this.spark = SparkContextUtil.getSparkSession(context, store.getProperties());
        SparkParquetUtils.configureSparkForAddElements(spark, store.getProperties());
        this.tempDir = store.getProperties().getTempFilesDir();
        // Locations for intermediate files
        this.groupToUnsortedUnaggregatedNewData =
                group -> tempDir + "/AddElementsFromRDDTemp/unsorted_unaggregated_new/group=" + group + "/";
        this.groupToUnsortedAggregatedNewData =
                group -> tempDir + "/AddElementsFromRDDTemp/unsorted_aggregated_new/group=" + group + "/";
        this.groupToSortedAggregatedNewData =
                group -> tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/group=" + group + "/";
        this.groupToSortedAggregatedNewDataReversed =
                group -> tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/reversed-group=" + group + "/";
    }

    void addElementsFromRDD(final JavaRDD<Element> input) throws OperationException {
        writeInputData(input);
        aggregateNewAndOldData();
        sort();
        sortEdgeGroupsByDestination();
        calculateAndWritePartitioner();
        createNewSnapshotDirectory();
        deleteTempDirectory();
    }

    void addElementsFromRDD(final RDD<Element> input) throws OperationException {
        addElementsFromRDD(input.toJavaRDD());
    }

    private void writeInputData(final JavaRDD<Element> input) {
        // Write data from input to unsorted files (for each partition of input, there will be one file per group)
        LOGGER.info("Writing data for input RDD");
        input.foreachPartition(new WriteData(groupToUnsortedUnaggregatedNewData, schema));
    }

    private void aggregateNewAndOldData() throws OperationException {
        // Aggregate the new data and old data together (one job for each group)
        LOGGER.info("Creating AggregateDataForGroup tasks for groups that require aggregation");
        for (final String group : schema.getAggregatedGroups()) {
            LOGGER.info("Creating AggregateDataForGroup task for group {}", group);
            final List<String> inputFiles = new ArrayList<>();
            final String groupDirectoryNewData = groupToUnsortedUnaggregatedNewData.apply(group);
            final FileStatus[] newData;
            try {
                newData = fs.listStatus(new Path(groupDirectoryNewData), path -> path.getName().endsWith(".parquet"));
            } catch (final IOException e) {
                throw new OperationException("IOException finding Parquet files in " + groupDirectoryNewData, e);
            }
            Arrays.stream(newData).map(f -> f.getPath().toString()).forEach(inputFiles::add);
            final List<Path> existingData;
            try {
                existingData = store.getFilesForGroup(group);
            } catch (final IOException e) {
                throw new OperationException("IOException finding files for group " + group, e);
            }
            existingData.stream().map(p -> p.toString()).forEach(inputFiles::add);
            final String outputDir = groupToUnsortedAggregatedNewData.apply(group);
            final AggregateDataForGroup aggregateDataForGroup;
            try {
                aggregateDataForGroup = new AggregateDataForGroup(fs, schemaUtils, group, inputFiles, outputDir, spark);
            } catch (final SerialisationException e) {
                throw new OperationException("SerialisationException creating AggregateDataForGroup task", e);
            }
            LOGGER.info("AggregateDataForGroup task for group {} is being called ({} files as input, outputting to {})",
                    group, inputFiles.size(), outputDir);
            aggregateDataForGroup.call();
        }
    }

    private void sort() throws OperationException {
        try {
            // Sort the data (one job for each group)
            for (final String group : schemaUtils.getGroups()) {
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
                sort(group, false, inputFiles, outputDir);
            }
        } catch (final IOException e) {
            throw new OperationException("IOException sorting data", e);
        }
    }

    private void sortEdgeGroupsByDestination() throws OperationException {
        try {
            // For each edge group, sort by destination to create reversed edges files
            for (final String group : schemaUtils.getEdgeGroups()) {
                final List<String> inputFiles = new ArrayList<>();
                final String inputDir = groupToSortedAggregatedNewData.apply(group);
                final FileStatus[] inputFilesFS = fs
                        .listStatus(new Path(inputDir), path -> path.getName().endsWith(".parquet"));
                Arrays.stream(inputFilesFS).map(f -> f.getPath().toString()).forEach(inputFiles::add);
                final String outputDir = groupToSortedAggregatedNewDataReversed.apply(group);
                sort(group, true, inputFiles, outputDir);
            }
        } catch (final IOException e) {
            throw new OperationException("IOException sorting edge groups by destination", e);
        }
    }

    private void sort(final String group, final boolean reversed, final List<String> inputFiles, final String outputDir)
            throws OperationException {
        try {
            final List<String> sortColumns = schemaUtils.columnsToSortBy(group, reversed);
            LOGGER.info("SortFullGroup task for {} group {} is being called ({} files as input, outputting to {})",
                    reversed ? "reversed" : "", group, inputFiles.size(), outputDir);
            new SortFullGroup(group,
                    schemaUtils.getEntityGroups().contains(group),
                    reversed,
                    schemaUtils,
                    sortColumns,
                    inputFiles,
                    outputDir,
                    store.getProperties().getAddElementsOutputFilesPerGroup(),
                    spark,
                    fs).call();
        } catch (final IOException e) {
            throw new OperationException("TODO");
        }
    }

    private void calculateAndWritePartitioner() throws OperationException {
        // Create new graph partitioner
        LOGGER.info("Calculating new GraphPartitioner");
        final GraphPartitioner newPartitioner;
        try {
            newPartitioner = new CalculatePartitioner(
                    new Path(tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/"), store.getSchema(), fs).call();
        } catch (final IOException e) {
            throw new OperationException("IOException calculating new graph partitioner", e);
        }
        LOGGER.info("New GraphPartitioner has partitions for {} groups, {} reversed edge groups",
                newPartitioner.getGroups().size(), newPartitioner.getGroupsForReversedEdges().size());

        // Write out graph partitioner
        Path newGraphPartitionerPath = null;
        try {
            newGraphPartitionerPath = new Path(tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/graphPartitioner");
            final FSDataOutputStream stream = fs.create(newGraphPartitionerPath);
            LOGGER.info("Writing graph partitioner to {}", newGraphPartitionerPath);
            new GraphPartitionerSerialiser().write(newPartitioner, stream);
            stream.close();
        } catch (final IOException e) {
            throw new OperationException("IOException writing out graph partitioner to " + newGraphPartitionerPath, e);
        }
    }

    private void createNewSnapshotDirectory() throws OperationException {
        // Move results to a new snapshot directory (the -tmp at the end allows us to add data to the directory,
        // and then when this is all finished we rename the directory to remove the -tmp; this allows us to make
        // the replacement of the old data with the new data an atomic operation and ensures that a get operation
        // against the store will not read the directory when only some of the data has been moved there).
        final long snapshot = System.currentTimeMillis();
        final String newDataDir = store.getDataDir() + "/" + ParquetStore.getSnapshotPath(snapshot) + "-tmp/";
        LOGGER.info("Moving aggregated and sorted data to new snapshot directory {}", newDataDir);
        LOGGER.info("Making directory {}", newDataDir);
        try {
            fs.mkdirs(new Path(newDataDir));
            final FileStatus[] fss = fs.listStatus(new Path(tempDir + "/AddElementsFromRDDTemp/sorted_aggregated_new/"));
            for (int i = 0; i < fss.length; i++) {
                final Path destination = new Path(newDataDir, fss[i].getPath().getName());
                fs.rename(fss[i].getPath(), destination);
                LOGGER.info("Renamed {} to {}", fss[i].getPath(), destination);
            }

            // Move snapshot-tmp directory to snapshot
            final String directoryWithoutTmp = newDataDir.substring(0, newDataDir.lastIndexOf("-tmp"));
            LOGGER.info("Renaming {} to {}", newDataDir, directoryWithoutTmp);
            fs.rename(new Path(newDataDir), new Path(directoryWithoutTmp));
        } catch (final IOException e) {
            throw new OperationException("IOException moving files to new snapshot directory", e);
        }

        // Set snapshot on store to new value
        LOGGER.info("Updating latest snapshot on store to {}", snapshot);
        try {
            store.setLatestSnapshot(snapshot);
        } catch (final StoreException e) {
            throw new OperationException("StoreException setting the latest snapshot on the store to " + snapshot, e);
        }
    }

    private void deleteTempDirectory() throws OperationException {
        // Delete temporary data directory
        LOGGER.info("Deleting temporary directory {}", new Path(tempDir));
        try {
            fs.delete(new Path(tempDir), true);
        } catch (final IOException e) {
            throw new OperationException("IOException deleting temporary directory " + tempDir, e);
        }
    }
}
