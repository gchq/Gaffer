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
package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Used to store commonly used methods to remove duplication of code throughout the Parquet store
 */
public final class ParquetStoreUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStoreUtils.class);

    private ParquetStoreUtils() {
    }

    public static ExecutorService createThreadPool(final SparkSession spark, final ParquetStoreProperties storeProperties) {
        final int numberOfThreads;
        final Option<String> sparkDriverCores = spark.conf().getOption("spark.driver.cores");
        if (sparkDriverCores.nonEmpty()) {
            numberOfThreads = Integer.parseInt(sparkDriverCores.get());
        } else {
            numberOfThreads = storeProperties.getThreadsAvailable();
        }
        LOGGER.debug("Created thread pool of size {} to aggregate and sort data", numberOfThreads);
        return Executors.newFixedThreadPool(numberOfThreads);
    }

    public static void invokeSplitPointCalculations(final ExecutorService pool,
                                                    final List<Callable<Tuple2<String, Map<Object, Integer>>>> tasks,
                                                    final Map<String, Map<Object, Integer>> groupToSplitPoints) throws OperationException {
        try {
            List<Future<Tuple2<String, Map<Object, Integer>>>> results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                final Tuple2<String, Map<Object, Integer>> result = results.get(i).get();
                if (null != result) {
                    final Map<Object, Integer> splitPoints = result._2;
                    if (!splitPoints.isEmpty()) {
                        groupToSplitPoints.put(result._1, splitPoints);
                    }
                }
            }
        } catch (final Exception e) {
            throw new OperationException(e.getMessage(), e);
        }
    }
}
