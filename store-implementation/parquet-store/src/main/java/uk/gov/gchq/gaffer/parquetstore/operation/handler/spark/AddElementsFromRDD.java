/*
 * Copyright 2017-2019 Crown Copyright
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
 * Executes the process of importing a {@link JavaRDD} or {@link RDD} of {@link Element}s into a {@link ParquetStore}.
 */
public class AddElementsFromRDD {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromRDD.class);

    private final ParquetStore store;
    private final Schema schema;
    private final SchemaUtils schemaUtils;
    private final FileSystem fs;
    private final SparkSession spark;
    private final String tempDir;

    AddElementsFromRDD(final Context context, final ParquetStore store) {
        this.store = store;
        this.schema = store.getSchema();
        this.schemaUtils = store.getSchemaUtils();
        this.fs = store.getFS();
        this.spark = SparkContextUtil.getSparkSession(context, store.getProperties());
        SparkParquetUtils.configureSparkForAddElements(spark, store.getProperties());
        this.tempDir = store.getProperties().getTempFilesDir();
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

    private String getDirectory(final String group, final boolean sorted, final boolean aggregated, final boolean reversed) {
        return tempDir
                + "/AddElementsFromRDDTemp/"
                + (sorted ? "sorted" : "unsorted")
                + "_"
                + (aggregated ? "aggregated" : "unaggregated")
                + "_new/"
                + (reversed ? "reversedEdges/" : "graph/")
                + (null != group ? "group=" + group + "/" : "");
    }

    private String getSortedAggregatedDirectory(final boolean sorted, final boolean aggregated) {
        return tempDir
                + "/AddElementsFromRDDTemp/"
                + (sorted ? "sorted" : "unsorted")
                + "_"
                + (aggregated ? "aggregated" : "unaggregated")
                + "_new/";
    }

    /**
     * Writes the provided {@link JavaRDD} of {@link Element}s to files split by group and partition. The data is
     * written in the order the {@link JavaRDD} provides it, with no sorting or aggregation.
     *
     * @param input the JavaRDD of Elements
     */
    private void writeInputData(final JavaRDD<Element> input) {
        LOGGER.info("Writing data for input RDD");
        final Function<String, String> groupToUnsortedUnaggregatedNewData =
                group -> getDirectory(group, false, false, false);
        input.foreachPartition(new WriteData(groupToUnsortedUnaggregatedNewData, schema, store.getProperties().getCompressionCodecName()));
    }

    /**
     * For each group that requires aggregation, this method aggregates the new data that has been written out to file
     * with the existing data for that group.
     *
     * @throws OperationException if an {@link IOException} or a {@link SerialisationException} is thrown
     */
    private void aggregateNewAndOldData() throws OperationException {
        LOGGER.info("Creating AggregateDataForGroup tasks for groups that require aggregation");
        for (final String group : schema.getAggregatedGroups()) {
            LOGGER.info("Creating AggregateDataForGroup task for group {}", group);
            final List<String> inputFiles = new ArrayList<>();
            final String groupDirectoryNewData = getDirectory(group, false, false, false);
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
            existingData.stream().map(Path::toString).forEach(inputFiles::add);
            final String outputDir = getDirectory(group, false, true, false);
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

    /**
     * For each group, sorts the data. If the group requires aggregation then the aggregated data from the previous
     * call to {@link AddElementsFromRDD#aggregateNewAndOldData} is sorted. If the group does not require aggregation
     * then the existing data and the new data are sorted in one operation.
     *
     * @throws OperationException if an {@link IOException} is thrown
     */
    private void sort() throws OperationException {
        try {
            for (final String group : schemaUtils.getGroups()) {
                final List<String> inputFiles = new ArrayList<>();
                final String outputDir = getDirectory(group, true, true, false);
                if (schemaUtils.getGafferSchema().getAggregatedGroups().contains(group)) {
                    final String inputDir = getDirectory(group, false, true, false);
                    final FileStatus[] inputFilesFS = fs
                            .listStatus(new Path(inputDir), path -> path.getName().endsWith(".parquet"));
                    Arrays.stream(inputFilesFS).map(f -> f.getPath().toString()).forEach(inputFiles::add);
                } else {
                    // Input is new data from groupToUnsortedUnaggregatedNewData and old data from existing snapshot directory
                    final String groupDirectoryNewData = getDirectory(group, false, false, false);
                    final FileStatus[] newData = fs
                            .listStatus(new Path(groupDirectoryNewData), path -> path.getName().endsWith(".parquet"));
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

    /**
     * Sorts all the edge groups by destination then source.
     *
     * @throws OperationException if an {@link IOException} is thrown
     */
    private void sortEdgeGroupsByDestination() throws OperationException {
        try {
            // For each edge group, sort by destination to create reversed edges files
            for (final String group : schemaUtils.getEdgeGroups()) {
                final List<String> inputFiles = new ArrayList<>();
                final String inputDir = getDirectory(group, true, true, false);
                final FileStatus[] inputFilesFS = fs
                        .listStatus(new Path(inputDir), path -> path.getName().endsWith(".parquet"));
                Arrays.stream(inputFilesFS).map(f -> f.getPath().toString()).forEach(inputFiles::add);
                final String outputDir = getDirectory(group, true, true, true);
                sort(group, true, inputFiles, outputDir);
            }
        } catch (final IOException e) {
            throw new OperationException("IOException sorting edge groups by destination", e);
        }
    }

    /**
     * Sorts the provided data.
     *
     * @param group the group
     * @param reversed whether these are edges that need to be sorted by destination then source
     * @param inputFiles the input files
     * @param outputDir the directory to write the output to
     * @throws OperationException if an {@link IOException} is thrown
     */
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
                    store.getProperties().getCompressionCodecName(),
                    spark,
                    fs).call();
        } catch (final IOException e) {
            throw new OperationException("TODO");
        }
    }

    /**
     * Calculates the new graph partitioner and writes it to file.
     *
     * @throws OperationException if an {@link IOException} is thrown
     */
    private void calculateAndWritePartitioner() throws OperationException {
        // Create new graph partitioner
        LOGGER.info("Calculating new GraphPartitioner");
        final GraphPartitioner newPartitioner;
        try {
            newPartitioner = new CalculatePartitioner(
                    new Path(getSortedAggregatedDirectory(true, true)),
                    store.getSchema(), fs).call();
        } catch (final IOException e) {
            throw new OperationException("IOException calculating new graph partitioner", e);
        }
        LOGGER.info("New GraphPartitioner has partitions for {} groups, {} reversed edge groups",
                newPartitioner.getGroups().size(), newPartitioner.getGroupsForReversedEdges().size());

        // Write out graph partitioner
        Path newGraphPartitionerPath = null;
        try {
            newGraphPartitionerPath = new Path(getSortedAggregatedDirectory(true, true) + "graphPartitioner");
            final FSDataOutputStream stream = fs.create(newGraphPartitionerPath);
            LOGGER.info("Writing graph partitioner to {}", newGraphPartitionerPath);
            new GraphPartitionerSerialiser().write(newPartitioner, stream);
            stream.close();
        } catch (final IOException e) {
            throw new OperationException("IOException writing out graph partitioner to " + newGraphPartitionerPath, e);
        }
    }

    /**
     * Creates a new snapshot directory within the data directory in the store and moves the new data there.
     *
     * This is done by first creating the new snapshot directory with "-tmp" at the end, then all files are moved
     * into that directory, and then the directory is renamed so that the "-tmp" is removed. This allows us to make
     * the replacement of the old data with the new data an atomic operation and ensures that a get operation
     * against the store will not read the directory when only some of the data has been moved there.
     *
     * @throws OperationException if an {@link IOException} or {@link StoreException} is thrown
     */
    private void createNewSnapshotDirectory() throws OperationException {
        final long snapshot = System.currentTimeMillis();
        final String newDataDir = store.getDataDir() + "/" + ParquetStore.getSnapshotPath(snapshot) + "-tmp/";
        LOGGER.info("Moving aggregated and sorted data to new snapshot-tmp directory {}", newDataDir);
        try {
            fs.mkdirs(new Path(newDataDir));
            final FileStatus[] fss = fs.listStatus(new Path(getSortedAggregatedDirectory(true, true)));
            for (int i = 0; i < fss.length; i++) {
                final Path destination = new Path(newDataDir, fss[i].getPath().getName());
                LOGGER.debug("Renaming {} to {}", fss[i].getPath(), destination);
                fs.rename(fss[i].getPath(), destination);
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

    /**
     * Deletes the temporary directory used for temporary data.
     *
     * @throws OperationException if an {@link IOException} is thrown
     */
    private void deleteTempDirectory() throws OperationException {
        LOGGER.info("Deleting temporary directory {}", new Path(tempDir));
        try {
            fs.delete(new Path(tempDir), true);
        } catch (final IOException e) {
            throw new OperationException("IOException deleting temporary directory " + tempDir, e);
        }
    }
}
