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

package uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Creates parallel tasks aggregate and sort the unsorted data for a given group and indexed column. For example if you
 * had a single edge group then it will generate two tasks:
 * The first would aggregate the group's unsorted data and then sort it by the SOURCE columns.
 * The second would again aggregate the same groups data and then sort it by the DESTINATION column.
 */
public class AggregateAndSortTempData {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateAndSortTempData.class);

    public AggregateAndSortTempData(final ParquetStore store, final SparkSession spark, final Map<String, Map<Integer, Object>> groupToSplitPoints) throws OperationException, SerialisationException {
        final List<Callable<OperationException>> tasks = new ArrayList<>();
        final SchemaUtils schemaUtils = store.getSchemaUtils();
        final GraphIndex index = store.getGraphIndex();
        final String currentDataDir;
        if (index != null) {
            currentDataDir = store.getDataDir()
                    + "/" + index.getSnapshotTimestamp();
        } else {
            currentDataDir = null;
        }
        final int numberOfSplits = store.getProperties().getAddElementsOutputFilesPerGroup();
        for (final String group : schemaUtils.getEdgeGroups()) {
            final Map<Integer, Object> splitPoints = groupToSplitPoints.get(group);
            final String currentDataInThisGroupDir;
            if (currentDataDir != null) {
                currentDataInThisGroupDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, currentDataDir);
            } else {
                currentDataInThisGroupDir = null;
            }
            for (int i = 0; i < numberOfSplits; i++) {
                final Set<String> currentGraphFiles = new HashSet<>();
                if (currentDataInThisGroupDir != null) {
//                    if (splitPoints != null) {
                        currentGraphFiles.add(currentDataInThisGroupDir + "/part-" + zeroPad(String.valueOf(i), 5) + "-*.parquet");
//                        int x = i - 1;
//                        while (x >= 0 && splitPoints.get(x) == null) {
//                            currentGraphFiles.add(currentDataInThisGroupDir + "/part-" + zeroPad(String.valueOf(i), 5) + "-*.parquet");
//                            x--;
//                        }
//                    }
                }
                tasks.add(new AggregateGroup(group, ParquetStoreConstants.SOURCE, store, currentGraphFiles, spark, i));
            }
        }
        for (final String group : schemaUtils.getEntityGroups()) {
            final Map<Integer, Object> splitPoints = groupToSplitPoints.get(group);
            final String currentDataInThisGroupDir;
            if (currentDataDir != null) {
                currentDataInThisGroupDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.SOURCE, currentDataDir);
            } else {
                currentDataInThisGroupDir = null;
            }
            for (int i = 0; i < numberOfSplits; i++) {
                final Set<String> currentGraphFiles = new HashSet<>();
                if (currentDataInThisGroupDir != null) {

//                    if (splitPoints != null) {
                        currentGraphFiles.add(currentDataInThisGroupDir + "/part-" + zeroPad(String.valueOf(i), 5) + "-*.parquet");
//                        int x = i - 1;
//                        while (x >= 0 && splitPoints.get(x) == null) {
//                            currentGraphFiles.add(currentDataInThisGroupDir + "/part-" + zeroPad(String.valueOf(i), 5) + "-*.parquet");
//                            x--;
//                        }
//                    }
                }
                tasks.add(new AggregateGroup(group, ParquetStoreConstants.VERTEX, store, currentGraphFiles, spark, i));
            }
        }
        final int numberOfThreads;
        final Option<String> sparkDriverCores = spark.conf().getOption("spark.driver.cores");
        if (sparkDriverCores.nonEmpty()) {
            numberOfThreads = Integer.parseInt(sparkDriverCores.get());
        } else {
            numberOfThreads = store.getProperties().getThreadsAvailable();
        }
        final ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);
        LOGGER.debug("Created thread pool of size {} to aggregate and sort data", numberOfThreads);
        try {
            List<Future<OperationException>> results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                final OperationException result = results.get(i).get();
                if (result != null) {
                    throw result;
                }
            }
            // sort the aggregated data
            tasks.clear();
            for (final String group : schemaUtils.getEdgeGroups()) {
                tasks.add(new SortGroup(group, ParquetStoreConstants.SOURCE, store, spark));
            }
            for (final String group : schemaUtils.getEntityGroups()) {
                tasks.add(new SortGroup(group, ParquetStoreConstants.VERTEX, store, spark));
            }
            results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                results.get(i).get();
            }
            // duplicate edge groups data sorted by the Destination
            tasks.clear();
            for (final String group : schemaUtils.getEdgeGroups()) {
                tasks.add(new GenerateIndexedColumnSortedData(group, store, spark));
            }
            results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                final OperationException result = results.get(i).get();
                if (result != null) {
                    throw result;
                }
            }
            pool.shutdown();
        } catch (final InterruptedException e) {
            throw new OperationException("AggregateAndSortData was interrupted", e);
        } catch (final ExecutionException e) {
            throw new OperationException("AggregateAndSortData had an execution exception thrown", e);
        }
        pool.shutdown();
    }

    private String zeroPad(final String input, final int length) {
        StringBuilder temp = new StringBuilder(input);
        while (temp.length() < length) {
            temp.insert(0, "0");
        }
        return temp.toString();
    }
}
