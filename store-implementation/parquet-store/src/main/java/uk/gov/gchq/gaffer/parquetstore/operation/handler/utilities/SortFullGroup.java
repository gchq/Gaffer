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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.Seq;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.io.reader.ParquetElementReader;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;

/**
 * This class is used to sort the data for a single group by loading in the /aggregate folder within a group.
 */
public class SortFullGroup implements Callable<OperationException> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortFullGroup.class);

    private final String group;
    private final boolean isEntity;
    private boolean isReversed;
    private final SchemaUtils schemaUtils;
    private final GafferGroupObjectConverter converter;
    private final List<String> sortColumns;
    private final List<String> inputFiles;
    private final String outputDir;
    private final int numberOfOutputFiles;
    private final SparkSession spark;
    private final FileSystem fs;

    public SortFullGroup(final String group,
                         final boolean isEntity,
                         final boolean isReversed,
                         final SchemaUtils schemaUtils,
                         final List<String> sortColumns,
                         final List<String> inputFiles,
                         final String outputDir,
                         final int numberOfOutputFiles,
                         final SparkSession spark,
                         final FileSystem fs) {
        this.group = group;
        this.isEntity = isEntity;
        this.isReversed = isReversed;
        this.schemaUtils = schemaUtils;
        this.converter = schemaUtils.getConverter(group);
        this.sortColumns = sortColumns;
        this.inputFiles = inputFiles;
        this.outputDir = outputDir;
        this.numberOfOutputFiles = numberOfOutputFiles;
        this.spark = spark;
        this.fs = fs;
    }

    @Override
    public OperationException call() throws IOException {
        final String firstSortColumn = sortColumns.get(0);
        final List<String> otherSortColumns = sortColumns.subList(1, sortColumns.size());
        final List<String> inputFilesThatExist = new ArrayList<>();
        for (final String file : inputFiles) {
            if (!fs.exists(new Path(file))) {
                LOGGER.info("Ignoring file {} as it does not exist", file);
            } else {
                inputFilesThatExist.add(file);
            }
        }
        if (inputFilesThatExist.isEmpty()) {
            LOGGER.info("Not sorting data for group {} as list of input files that exist is empty", group);
            return null;
        }

        // Partition by core columns (e.g. source, destination, directed for an edge) and then sort within partitions
        // by core columns and group-by columns. This ensures that all data about an edge ends up in one partition
        // but within that partition it is sorted by the core columns and the group-by columns. If we just sort by
        // the core and group-by columns then we can have the same edge split across multiple partitions (which
        // breaks our partitioning approach and would make it difficult to do query-time aggregation).

        LOGGER.info("Sorting data in {} files by columns {} to {} files in output directory {}",
            inputFilesThatExist.size(), StringUtils.join(sortColumns, ','), numberOfOutputFiles, outputDir);

        // NB: Don't want to include group-by columns as need to partition by core properties only (e.g. source, destination, directed)
        final ExtractKeyFromRow extractKeyFromRow = new ExtractKeyFromRow(new HashSet<>(),
                schemaUtils.getColumnToPaths(group), schemaUtils.getEntityGroups().contains(group), isReversed);

        final List<Seq<Object>> rows = spark.read()
                .option("mergeSchema", true)
                .parquet(inputFilesThatExist.toArray(new String[]{}))
                .javaRDD()
                .map(extractKeyFromRow)
                .takeSample(false, 10000, 1234567890L);

        final TreeSet<Seq<Object>> sortedRows = new TreeSet<>(new SeqComparator());
        sortedRows.addAll(rows);

        final TreeSet<Seq<Object>> splitPoints = new TreeSet<>(new SeqComparator());

        int desiredNumberOfSplits = numberOfOutputFiles - 1;
        long outputEveryNthRecord;
        if (sortedRows.size() < 2 || desiredNumberOfSplits < 1) {
            outputEveryNthRecord = 1;
        } else {
            outputEveryNthRecord = sortedRows.size() / desiredNumberOfSplits;
        }

        if (outputEveryNthRecord < 1) {
            outputEveryNthRecord = 1;
        }

        int numberOfSplitsOutput = 0;
        int count = 0;

        for (final Seq<Object> seq : sortedRows) {
            count++;
            if (0 == count % outputEveryNthRecord) {
                splitPoints.add(seq);
                numberOfSplitsOutput++;
            }
            if (numberOfSplitsOutput >= desiredNumberOfSplits) {
                break;
            }
        }

        final SeqObjectPartitioner partitioner = new SeqObjectPartitioner(numberOfOutputFiles, splitPoints);

        final JavaRDD<Row> x = spark.read()
                .option("mergeSchema", true)
                .parquet(inputFilesThatExist.toArray(new String[]{}))
                .javaRDD()
                .keyBy(new ExtractKeyFromRow(new HashSet<>(),
                        schemaUtils.getColumnToPaths(group), schemaUtils.getEntityGroups().contains(group), isReversed))
                .partitionBy(partitioner)
                .values();

        spark.createDataFrame(x, schemaUtils.getSparkSchema(group))
                .sortWithinPartitions(firstSortColumn, otherSortColumns.stream().toArray(String[]::new))
                .write()
                .option("compression", "gzip")
                .parquet(outputDir);

        // Rename files, e.g. part-00000-*** to partition-0, removing empty files and adapting numbers accordingly
        LOGGER.info("Renaming part-* files to partition-* files, removing empty files");
        final FileStatus[] sortedFiles = fs
                .listStatus(new Path(outputDir), path -> path.getName().endsWith(".parquet"));
        int counter = 0;
        for (int i = 0; i < sortedFiles.length; i++) {
            final Path path = sortedFiles[i].getPath();
            final boolean isEmpty = isFileEmpty(path);
            if (isEmpty) {
                LOGGER.debug("Deleting empty file {}", path);
                fs.delete(path, false);
            } else {
                final Path newPath = new Path(outputDir + ParquetStore.getFile(counter));
                LOGGER.debug("Renaming {} to {}", path, newPath);
                fs.rename(sortedFiles[i].getPath(), newPath);
                // NB This automatically renames the .crc file as well
                counter++;
            }
        }
        return null;
    }

    private boolean isFileEmpty(final Path path) throws IOException {
        LOGGER.debug("Opening a new Parquet reader for file {} to test if it's empty", path);
        final ParquetReader<Element> reader = new ParquetElementReader.Builder<Element>(path)
                .isEntity(isEntity)
                .usingConverter(converter)
                .build();
        boolean isEmpty = true;
        if (null != reader.read()) {
            isEmpty = false;
        }
        reader.close();
        LOGGER.debug("File {} is {}", path, isEmpty ? "empty" : "not empty");
        return isEmpty;
    }

    public static class SeqComparator implements Comparator<Seq<Object>> {

        @Override
        public int compare(final Seq<Object> seq1, final Seq<Object> seq2) {
            Iterator<Object> seq1Iterator = scala.collection.JavaConversions.asJavaIterator(seq1.iterator());
            Iterator<Object> seq2Iterator = scala.collection.JavaConversions.asJavaIterator(seq2.iterator());
            while (seq1Iterator.hasNext()) {
                final Comparable o1 = (Comparable) seq1Iterator.next();
                if (!seq2Iterator.hasNext()) {
                    throw new RuntimeException("Should be comparing two Seqs of equal size, got " + seq1 + " and " + seq2);
                }
                final Comparable o2 = (Comparable) seq2Iterator.next();
                final int comparison = o1.compareTo(o2);
                if (0 != comparison) {
                    return comparison;
                }
            }
            return 0;
        }
    }
}
