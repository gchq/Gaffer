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

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.index.ColumnIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GroupIndex;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.koryphe.tuple.n.Tuple4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Generates and processes the tasks ({@link GenerateIndexForColumnGroup}) that will generate the index for each sorted
 * directories data.
 */
public class GenerateIndices {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateIndices.class);
    private final GraphIndex graphIndex;

    public GenerateIndices(final ParquetStore store, final SparkSession spark) throws OperationException, SerialisationException, StoreException {
        graphIndex = new GraphIndex();
        final int numberOfThreads;
        final Option<String> sparkDriverCores = spark.conf().getOption("spark.driver.cores");
        if (sparkDriverCores.nonEmpty()) {
            numberOfThreads = Integer.parseInt(sparkDriverCores.get());
        } else {
            numberOfThreads = store.getProperties().getThreadsAvailable();
        }
        final ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);
        final String tempFileDir = store.getTempFilesDir();
        final SchemaUtils schemaUtils = store.getSchemaUtils();
        final String rootDir = tempFileDir + "/" + ParquetStoreConstants.SORTED;
        final List<Callable<Tuple4<String, String, ColumnIndex, OperationException>>> tasks = new ArrayList<>();
        for (final String group : schemaUtils.getEntityGroups()) {
            final String directory = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, rootDir);
            tasks.add(new GenerateIndexForColumnGroup(directory, schemaUtils.getPaths(group, ParquetStoreConstants.VERTEX), group, ParquetStoreConstants.VERTEX, spark));
            LOGGER.debug("Created a task to create the graphIndex for group {} from directory {} and paths {}",
                    group, directory, schemaUtils.getPaths(group, ParquetStoreConstants.VERTEX));
        }
        for (final String group : store.getSchemaUtils().getEdgeGroups()) {
            final Map<String, String[]> columnToPaths = schemaUtils.getColumnToPaths(group);
            final String directorySource = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.SOURCE, rootDir);
            LOGGER.debug("Creating a task to create the graphIndex for group {} from directory {} and paths {}",
                    group, directorySource, StringUtils.join(columnToPaths.get(ParquetStoreConstants.SOURCE)));
            tasks.add(new GenerateIndexForColumnGroup(directorySource, columnToPaths.get(ParquetStoreConstants.SOURCE), group, ParquetStoreConstants.SOURCE, spark));
            final String directoryDestination = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.DESTINATION, rootDir);
            LOGGER.debug("Creating a task to create the graphIndex for group {} from directory {} and paths {}",
                    group, directorySource, StringUtils.join(columnToPaths.get(ParquetStoreConstants.DESTINATION)));
            tasks.add(new GenerateIndexForColumnGroup(directoryDestination, columnToPaths.get(ParquetStoreConstants.DESTINATION), group, ParquetStoreConstants.DESTINATION, spark));
        }

        try {
            final List<Future<Tuple4<String, String, ColumnIndex, OperationException>>> results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                final Tuple4<String, String, ColumnIndex, OperationException> result = results.get(i).get();
                final OperationException error = result.get3();
                if (null != error) {
                    throw error;
                }
                final ColumnIndex colIndex = result.get2();
                if (!colIndex.isEmpty()) {
                    addColumnIndexToGraphIndex(result.get2(), result.get0(), result.get1());
                }
            }
            graphIndex.writeGroups(rootDir, store.getFS());
            pool.shutdown();
        } catch (final InterruptedException e) {
            throw new OperationException("GenerateIndices was interrupted", e);
        } catch (final ExecutionException e) {
            throw new OperationException("GenerateIndices had an execution exception thrown", e);
        }
    }

    private void addColumnIndexToGraphIndex(final ColumnIndex columnIndex, final String group, final String column) {
        GroupIndex groupIndex = graphIndex.getGroup(group);
        if (null == groupIndex) {
            groupIndex = new GroupIndex();
            graphIndex.add(group, groupIndex);
        }
        groupIndex.add(column, columnIndex);
    }

    public GraphIndex getGraphIndex() {
        return graphIndex;
    }
}
