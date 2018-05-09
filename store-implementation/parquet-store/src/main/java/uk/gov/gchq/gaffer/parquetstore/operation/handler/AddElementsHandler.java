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
package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.AggregateAndSortTempData;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.CalculateSplitPointsFromIndex;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.CalculateSplitPointsFromIterable;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.GenerateIndices;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.WriteUnsortedData;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SparkParquetUtils;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreUtils.createThreadPool;
import static uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreUtils.invokeSplitPointCalculations;

/**
 * An {@link OperationHandler} for the {@link AddElements} operation on the {@link ParquetStore}.
 */
public class AddElementsHandler implements OperationHandler<AddElements> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsHandler.class);

    @Override
    public Void doOperation(final AddElements operation,
                            final Context context,
                            final Store store) throws OperationException {
        final SparkSession spark = SparkContextUtil.getSparkSession(context, store.getProperties());
        final ParquetStore parquetStore = (ParquetStore) store;
        SparkParquetUtils.configureSparkForAddElements(spark, parquetStore.getProperties());
        addElements(operation, parquetStore, spark);
        return null;
    }

    private void addElements(final AddElements addElementsOperation, final ParquetStore store, final SparkSession spark)
            throws OperationException {
        try {
            final FileSystem fs = store.getFS();
            final ParquetStoreProperties parquetStoreProperties = store.getProperties();
            final Schema gafferSchema = store.getSchema();
            final String rootDataDirString = store.getDataDir();
            final String tempDirString = store.getTempFilesDir();
            final Path tempDir = new Path(tempDirString);
            if (fs.exists(tempDir)) {
                fs.delete(tempDir, true);
                LOGGER.warn("Temp data directory '{}' has been deleted.", tempDirString);
            }
            // Write the data out
            LOGGER.info("Starting to write the input Parquet data to {} split by group and split points", tempDirString);
            final Iterable<? extends Element> input = addElementsOperation.getInput();
            final ExecutorService pool = createThreadPool(spark, parquetStoreProperties);
            final List<Callable<Tuple2<String, Map<Object, Integer>>>> tasks = new ArrayList<>();
            final Map<String, Map<Object, Integer>> groupToSplitPoints;
            final GraphIndex index = store.getGraphIndex();
            if (null == index) {
                groupToSplitPoints = new HashMap<>();
                for (final String group : gafferSchema.getEdgeGroups()) {
                    tasks.add(new CalculateSplitPointsFromIterable(parquetStoreProperties.getSampleRate(),
                            parquetStoreProperties.getAddElementsOutputFilesPerGroup() - 1, input, group, false));
                }
                for (final String group : gafferSchema.getEntityGroups()) {
                    tasks.add(new CalculateSplitPointsFromIterable(parquetStoreProperties.getSampleRate(),
                            parquetStoreProperties.getAddElementsOutputFilesPerGroup() - 1, input, group, true));
                }
                invokeSplitPointCalculations(pool, tasks, groupToSplitPoints);
            } else {
                groupToSplitPoints = CalculateSplitPointsFromIndex.apply(index, store.getSchemaUtils(), parquetStoreProperties, input, pool);
            }

            final Iterator<? extends Element> inputIter = input.iterator();
            new WriteUnsortedData(store, groupToSplitPoints).writeElements(inputIter);
            if (inputIter instanceof CloseableIterator) {
                ((CloseableIterator) inputIter).close();
            }
            if (input instanceof CloseableIterable) {
                ((CloseableIterable) input).close();
            }
            LOGGER.debug("Finished writing the input Parquet data to {}", tempDirString);
            // Use to Spark read in all the data, aggregate and sort it
            LOGGER.debug("Starting to write the sorted and aggregated Parquet data to {}/sorted split by group", tempDirString);
            new AggregateAndSortTempData(store, spark, groupToSplitPoints, pool);
            pool.shutdown();
            LOGGER.debug("Finished writing the sorted and aggregated Parquet data to {}/sorted", tempDirString);
            // Generate the file based index
            LOGGER.debug("Starting to write the indexes");
            final GraphIndex newGraphIndex = new GenerateIndices(store, spark).getGraphIndex();
            LOGGER.debug("Finished writing the indexes");
            try {
                moveDataToDataDir(store, fs, rootDataDirString, tempDirString, newGraphIndex);
                tidyUp(fs, tempDirString);
            } catch (final IOException | StoreException e) {
                throw new OperationException("Failed to reload the indices", e);
            }
        } catch (final IOException e) {
            throw new OperationException("IOException: Failed to connect to the file system", e);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }

    }

    private void moveDataToDataDir(final ParquetStore store, final FileSystem fs, final String dataDirString, final String tempDataDirString, final GraphIndex newGraphIndex) throws StoreException, IOException {
        // Move data from temp to data
        final long snapshot = System.currentTimeMillis();
        final String destPath = dataDirString + "/" + snapshot;
        LOGGER.debug("Creating directory {}", destPath);
        fs.mkdirs(new Path(destPath).getParent());
        final String tempPath = tempDataDirString + "/" + ParquetStoreConstants.SORTED;
        if (fs.exists(new Path(tempPath))) {
            LOGGER.debug("Renaming {} to {}", tempPath, destPath);
            fs.rename(new Path(tempPath), new Path(destPath));
        }
        // Reload indices
        newGraphIndex.setSnapshotTimestamp(snapshot);
        store.setGraphIndex(newGraphIndex);
    }

    private void tidyUp(final FileSystem fs, final String tempDataDirString) throws IOException {
        Path tempDir = new Path(tempDataDirString);
        fs.delete(tempDir, true);
        LOGGER.debug("Temp data directory '{}' has been deleted.", tempDataDirString);
        while (fs.listStatus(tempDir.getParent()).length == 0) {
            tempDir = tempDir.getParent();
            LOGGER.debug("Empty directory '{}' has been deleted.", tempDataDirString);
            fs.delete(tempDir, true);
        }
    }
}
