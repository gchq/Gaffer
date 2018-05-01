/*
 * Copyright 2017. Crown Copyright
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

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Creates parallel tasks aggregate and sort the unsorted data for a given group and indexed column. For example if you
 * had a single edge group then it will generate two tasks:
 * The first would aggregate the group's unsorted data and then sort it by the SOURCE columns.
 * The second would again aggregate the same groups data and then sort it by the DESTINATION column.
 */
public class AggregateAndSortTempData {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateAndSortTempData.class);
    private static final String SORTED = "/sorted";
    private static final String SPLIT = "/split";

    public AggregateAndSortTempData(final ParquetStore store, final SparkSession spark,
                                    final Map<String, Map<Object, Integer>> groupToSplitPoints,
                                    final ExecutorService pool) throws OperationException, SerialisationException {
        final List<Callable<OperationException>> tasks = new ArrayList<>();
        final SchemaUtils schemaUtils = store.getSchemaUtils();
        final GraphIndex index = store.getGraphIndex();
        final String currentDataDir;
        if (null != index) {
            currentDataDir = store.getDataDir()
                    + "/" + index.getSnapshotTimestamp();
        } else {
            currentDataDir = null;
        }
        LOGGER.debug("Starting to aggregate the input data with existing data");
        for (final String group : schemaUtils.getEdgeGroups()) {
            addAggregationTask(group, ParquetStoreConstants.SOURCE, currentDataDir, groupToSplitPoints, tasks, store, spark);
        }
        for (final String group : schemaUtils.getEntityGroups()) {
            addAggregationTask(group, ParquetStoreConstants.VERTEX, currentDataDir, groupToSplitPoints, tasks, store, spark);
        }
        try {
            List<Future<OperationException>> results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                final OperationException result = results.get(i).get();
                if (null != result) {
                    throw result;
                }
            }
            LOGGER.debug("Finished aggregating the data");
            // sort the aggregated data
            tasks.clear();
            LOGGER.debug("Starting to sort the aggregated data");
            final ParquetStoreProperties props = store.getProperties();
            for (final String group : schemaUtils.getEdgeGroups()) {
                if (groupToSplitPoints.containsKey(group)) {
                    final Collection<Integer> splits = groupToSplitPoints.get(group).values();
                    if (props.getSortBySplitsOnIngest()) {
                        tasks.add(new SortFullGroup(group, ParquetStoreConstants.DESTINATION, store, spark, splits.size()));
                        for (final int i : splits) {
                            tasks.add(new SortGroupSplit(group, ParquetStoreConstants.SOURCE, store, spark, i));
                        }
                    } else {
                        tasks.add(new SortFullGroup(group, ParquetStoreConstants.SOURCE, store, spark, props.getAddElementsOutputFilesPerGroup()));
                        tasks.add(new SortFullGroup(group, ParquetStoreConstants.DESTINATION, store, spark, props.getAddElementsOutputFilesPerGroup()));
                    }
                }
            }
            for (final String group : schemaUtils.getEntityGroups()) {
                if (groupToSplitPoints.containsKey(group)) {
                    final Collection<Integer> splits = groupToSplitPoints.get(group).values();
                    if (props.getSortBySplitsOnIngest()) {
                        for (final int i : splits) {
                            tasks.add(new SortGroupSplit(group, ParquetStoreConstants.VERTEX, store, spark, i));
                        }
                    } else {
                        tasks.add(new SortFullGroup(group, ParquetStoreConstants.VERTEX, store, spark, props.getAddElementsOutputFilesPerGroup()));
                    }
                }
            }
            results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                results.get(i).get();
            }
            LOGGER.debug("Finished sorting the aggregated data");
            // move sorted files into a directory ready to be moved to the data directory
            if (props.getSortBySplitsOnIngest()) {
                for (final String group : schemaUtils.getEdgeGroups()) {
                    if (groupToSplitPoints.containsKey(group)) {
                        final Collection<Integer> splits = groupToSplitPoints.get(group).values();
                        for (final int i : splits) {
                            moveData(store.getFS(), store.getTempFilesDir(), group, ParquetStoreConstants.SOURCE, String.valueOf(i));
                        }
                    }
                }
                for (final String group : schemaUtils.getEntityGroups()) {
                    if (groupToSplitPoints.containsKey(group)) {
                        final Collection<Integer> splits = groupToSplitPoints.get(group).values();
                        for (final int i : splits) {
                            moveData(store.getFS(), store.getTempFilesDir(), group, ParquetStoreConstants.VERTEX, String.valueOf(i));
                        }
                    }
                }
            }
        } catch (final InterruptedException e) {
            throw new OperationException("AggregateAndSortData was interrupted", e);
        } catch (final ExecutionException e) {
            throw new OperationException("AggregateAndSortData had an execution exception thrown", e);
        } catch (final StoreException e) {
            throw new OperationException("AggregateAndSortData had a store exception thrown", e);
        } catch (final IOException e) {
            throw new OperationException("AggregateAndSortData had an IO exception thrown", e);
        }
        pool.shutdown();
    }

    private void addAggregationTask(final String group,
                                    final String column,
                                    final String currentDataDir,
                                    final Map<String, Map<Object, Integer>> groupToSplitPoints,
                                    final List<Callable<OperationException>> tasks,
                                    final ParquetStore store,
                                    final SparkSession spark) throws SerialisationException {
        if (groupToSplitPoints.containsKey(group)) {
            final String currentDataInThisGroupDir;
            if (null != currentDataDir) {
                currentDataInThisGroupDir = ParquetStore.getGroupDirectory(group, column, currentDataDir);
            } else {
                currentDataInThisGroupDir = null;
            }
            final Collection<Integer> splits = groupToSplitPoints.get(group).values();
            for (final int i : splits) {
                final Set<String> currentGraphFiles = new HashSet<>();
                if (null != currentDataInThisGroupDir) {
                    currentGraphFiles.add(currentDataInThisGroupDir + "/part-" + zeroPad(String.valueOf(i), 5) + "*.parquet");
                }
                tasks.add(new AggregateGroupSplit(group, column, store, currentGraphFiles, spark, i));
            }
        }
    }

    private void moveData(final FileSystem fs, final String tempFileDir, final String group, final String column, final String splitNumber) throws StoreException, IOException, OperationException {
        // Move data from temp to data
        final String sourceFile = ParquetStore.getGroupDirectory(group, column, tempFileDir) + SORTED + SPLIT + splitNumber + "/part-00000-*.parquet";
        final FileStatus[] files = fs.globStatus(new Path(sourceFile));
        if (files.length == 1) {
            final Path destPath = new Path(ParquetStore.getGroupDirectory(group, column, tempFileDir + SORTED) + "/part-" + zeroPad(splitNumber, 5) + ".gz.parquet");
            fs.mkdirs(destPath.getParent());
            fs.rename(files[0].getPath(), destPath);
        } else if (files.length > 1) {
            throw new OperationException("Expected to get only one file which matched the file pattern " + sourceFile);
        }
    }

    private String zeroPad(final String input, final int length) {
        final StringBuilder temp = new StringBuilder(input);
        while (temp.length() < length) {
            temp.insert(0, "0");
        }
        return temp.toString();
    }
}
