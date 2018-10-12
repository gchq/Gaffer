/*
 * Copyright 2018. Crown Copyright
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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Aggregates and sorts the data in a list of files into one sorted file. All the input data should be from the same
 * file.
 */
public class AggregateAndSortData implements Callable<CallableResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateAndSortData.class);
    private static final String AGGREGATED = "/aggregated";
    private static final String SORTED = "/sorted";

    private final SchemaUtils schemaUtils;
    private final FileSystem fs;
    private final List<String> files;
    private final String outputDir;
    private final String group;
    private final String id; // Used in the logging statements so that users of this class can provide some context as to what is being done
    private final boolean reversed;
    private final SparkSession sparkSession;
    private final Set<String> groupsWithAggregation;

    public AggregateAndSortData(final SchemaUtils schemaUtils,
                                final FileSystem fs,
                                final List<String> files,
                                final String outputDir,
                                final String group,
                                final String id,
                                final boolean reversed,
                                final SparkSession sparkSession) {
        this.schemaUtils = schemaUtils;
        this.fs = fs;
        this.files = files;
        this.outputDir = outputDir;
        this.group = group;
        this.id = id;
        this.reversed = reversed;
        this.sparkSession = sparkSession;
        this.groupsWithAggregation = new HashSet<>(this.schemaUtils.getGafferSchema().getAggregatedGroups());
    }

    @Override
    public CallableResult call() throws Exception {
        final String sortedFiles = outputDir + SORTED;
        // Aggregate existing files
        if (!groupsWithAggregation.contains(group)) {
            LOGGER.info("Sorting data for group {} and id {} ({} input files, results will be stored in {})",
                    group, id, files.size(), sortedFiles);
            new SortGroupSplit(fs, sparkSession, schemaUtils.columnsToSortBy(group, reversed), files, sortedFiles).call();
        } else {
            final String aggregatedFiles = outputDir + AGGREGATED;
            LOGGER.info("Aggregating data for group {} and id {} ({} input files, results will be stored in {})",
                    group, id, files.size(), aggregatedFiles);
            final CallableResult result = new AggregateDataForGroup(fs, schemaUtils, group, files, aggregatedFiles, sparkSession).call();
            if (null != result) {
                LOGGER.info("Sorting aggregated data for group {} and id {} (results will be written to {})",
                        group, id, sortedFiles);
                new SortGroupSplit(fs, sparkSession, schemaUtils.columnsToSortBy(group, reversed),
                        aggregatedFiles, sortedFiles).call();
                LOGGER.info("Deleting aggregated files in {} for group {} and {}", aggregatedFiles, group, id);
                fs.delete(new Path(aggregatedFiles), true);
            } else {
                LOGGER.info("Not sorting aggregate data for group {} and id {} as there were no results", group, id);
            }
        }

        if (fs.exists(new Path(sortedFiles))) {
            LOGGER.info("Moving files of sorted data from {} to {} (group {}, id {})", sortedFiles, outputDir, group, id);
            final FileStatus[] files = fs.listStatus(new Path(sortedFiles));
            for (final FileStatus file : files) {
                final Path newPath = new Path(outputDir, file.getPath().getName());
                fs.rename(file.getPath(), newPath);
            }
        } else {
            LOGGER.info("No files of sorted data so there is nothing to move");
        }

        return CallableResult.SUCCESS;
    }
}
