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
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.OperationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class is used to sort the data for a single split of a group by loading in the /aggregate/split folder within a group.
 */
public class SortGroupSplit implements Callable<OperationException> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortGroupSplit.class);

    private final List<String> inputFiles;
    private final String outputDir;
    private final SparkSession spark;
    private final FileSystem fs;
    private final List<String> sortColumns;

    public SortGroupSplit(final FileSystem fs,
                          final SparkSession spark,
                          final List<String> sortColumns,
                          final String inputDir,
                          final String outputDir) throws IOException {
        this.fs = fs;
        this.sortColumns = sortColumns;
        if (!this.fs.exists(new Path(inputDir))) {
            throw new IOException("Input directory " + inputDir + " does not exist");
        }
        final FileStatus[] fileStatuses = this.fs
                .listStatus(new Path(inputDir), file1 -> file1.getName().endsWith(".parquet"));
        if (0 == fileStatuses.length) {
            LOGGER.info("Not performing SortGroupSplit for inputDir {} as it contains no Parquet files");
        }
        this.inputFiles = new ArrayList<>();
        for (final FileStatus file : fileStatuses) {
            this.inputFiles.add(file.getPath().toString());
        }
        this.outputDir = outputDir;
        this.spark = spark;
    }

    public SortGroupSplit(final FileSystem fs,
                          final SparkSession spark,
                          final List<String> sortColumns,
                          final List<String> inputFiles,
                          final String outputDir) {
        this.fs = fs;
        this.spark = spark;
        this.sortColumns = sortColumns;
        this.inputFiles = inputFiles;
        this.outputDir = outputDir;
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
            LOGGER.info("Not sorting data for group {} as list of input files that exist is empty");
            return null;
        }

        LOGGER.info("Sorting data in files {} by columns {} to one file in output directory {}",
                StringUtils.join(inputFilesThatExist, ','), StringUtils.join(sortColumns, ','), outputDir);
        spark.read()
                .option("mergeSchema", true)
                .parquet(inputFilesThatExist.toArray(new String[]{}))
                .sort(firstSortColumn, otherSortColumns.stream().toArray(String[]::new))
                .coalesce(1)
                .write()
                .option("compression", "gzip")
                .parquet(outputDir);
        return null;
    }
}
