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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AggregateAndSortTempData {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateAndSortTempData.class);

    public AggregateAndSortTempData(final ParquetStore store, final SparkSession spark) throws OperationException, SerialisationException {
        final List<Callable<OperationException>> tasks = new ArrayList<>();
        final SchemaUtils schemaUtils = store.getSchemaUtils();
        final ParquetStoreProperties parquetStoreProperties = store.getProperties();
        final String currentDataDir = parquetStoreProperties.getDataDir()
                + "/" + store.getIndex().getSnapshotTimestamp()
                + "/" + ParquetStoreConstants.GRAPH;
        for (final String group : schemaUtils.getEdgeGroups()) {
            final String currentDataInThisGroupDir = currentDataDir + "/" + ParquetStoreConstants.GROUP + "=" + group;
            tasks.add(new AggregateAndSortGroup(group, false, parquetStoreProperties, currentDataInThisGroupDir, schemaUtils, spark));
            tasks.add(new AggregateAndSortGroup(group, true, parquetStoreProperties, currentDataInThisGroupDir, schemaUtils, spark));
        }
        for (final String group : schemaUtils.getEntityGroups()) {
            final String currentDataInThisGroupDir = currentDataDir + "/" + ParquetStoreConstants.GROUP + "=" + group;
            tasks.add(new AggregateAndSortGroup(group, false, parquetStoreProperties, currentDataInThisGroupDir, schemaUtils, spark));
        }
        final ExecutorService pool = Executors.newFixedThreadPool(store.getProperties().getThreadsAvailable());
        LOGGER.info("Created thread pool of size {} to aggregate and sort data", store.getProperties().getThreadsAvailable());
        try {
            final List<Future<OperationException>> results = pool.invokeAll(tasks);
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
}
