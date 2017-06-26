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

package uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.AggregateAndSortTempData;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.GenerateIndices;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SparkParquetUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.WriteUnsortedDataFunction;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;

public class AddElementsFromRDDHandler implements OperationHandler<ImportRDDOfElements> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromRDDHandler.class);

    @Override
    public Void doOperation(final ImportRDDOfElements operation, final Context context, final Store store)
            throws OperationException {
        return addElementsFromRDD(operation, context, (ParquetStore) store);
    }

    private Void addElementsFromRDD(final ImportRDDOfElements operation, final Context context, final ParquetStore store)
            throws OperationException {
        try {
            final FileSystem fs = store.getFS();
            final ParquetStoreProperties parquetStoreProperties = store.getProperties();
            final String tempDataDirString = parquetStoreProperties.getTempFilesDir();
            final Path tempDir = new Path(tempDataDirString);
            final String rootDataDirString = parquetStoreProperties.getDataDir();
            final String dataDirString = rootDataDirString + "/" + store.getCurrentSnapshot();
            if (fs.exists(tempDir)) {
                fs.delete(tempDir, true);
                LOGGER.warn("Temp data directory '" + tempDataDirString + "' has been deleted.");
            }
            if (store.getCurrentSnapshot() != 0L) {
                FileUtil.copy(fs, new Path(dataDirString), fs, tempDir, false, false, fs.getConf());
                LOGGER.debug("Copying data directory '" + dataDirString + "' has been copied to " + tempDataDirString);
            }
            final User user = context.getUser();
            if (user instanceof SparkUser) {
                final SparkSession spark = ((SparkUser) user).getSparkSession();
                SparkParquetUtils.configureSparkForAddElements(spark, parquetStoreProperties);
                // Write the data out
                LOGGER.info("Starting to write the unsorted Parquet data to " + tempDataDirString + " split by group");
                final WriteUnsortedDataFunction writeUnsortedDataFunction =
                        new WriteUnsortedDataFunction(store.getSchemaUtils(), parquetStoreProperties);
                operation.getInput().foreachPartition(writeUnsortedDataFunction);
                LOGGER.info("Finished writing the unsorted Parquet data to " + tempDataDirString);
                // Spark read in the data, aggregate and sort the data
                LOGGER.info("Starting to write the sorted and aggregated Parquet data to " + tempDataDirString + " split by group");
                new AggregateAndSortTempData(store, spark);
                LOGGER.info("Finished writing the sorted and aggregated Parquet data to " + tempDataDirString);
                // Generate the file based index
                LOGGER.info("Starting to write the indexes");
                new GenerateIndices(store);
                LOGGER.info("Finished writing the indexes");
            } else {
                throw new OperationException("This operation requires the user to be of type SparkUser.");
            }
            try {
                moveDataToDataDir(store, fs, rootDataDirString, tempDataDirString);
                tidyUp(fs, tempDataDirString);
            } catch (final StoreException e) {
                throw new OperationException("Failed to reload the indices", e);
            } catch (final IOException e) {
                throw new OperationException("Failed to move data from temporary files directory to the data directory.", e);
            }
        } catch (final IOException e) {
            throw new OperationException("IOException: Failed to connect to the file system", e);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
        return null;
    }

    private void moveDataToDataDir(final ParquetStore store,
                                   final FileSystem fs,
                                   final String rootDataDirString,
                                   final String tempDataDirString) throws StoreException, IOException {
        // Move data from temp to data
        final long snapshot = System.currentTimeMillis();
        final String destPath = rootDataDirString + "/" + snapshot;
        fs.mkdirs(new Path(destPath));
        fs.rename(new Path(tempDataDirString + "/" + ParquetStoreConstants.SORTED + "/" + ParquetStoreConstants.GRAPH),
                new Path(destPath + "/" + ParquetStoreConstants.GRAPH));
        final Path tempReversePath = new Path(tempDataDirString
                + "/" + ParquetStoreConstants.SORTED
                + "/" + ParquetStoreConstants.REVERSE_EDGES);
        if (fs.exists(tempReversePath)) {
            fs.rename(tempReversePath, new Path(destPath + "/" + ParquetStoreConstants.REVERSE_EDGES));
        }
        // Set the data dir property
        store.setCurrentSnapshot(snapshot);
        // Reload indices
        store.loadIndices();
    }

    private void tidyUp(final FileSystem fs, final String tempDataDirString) throws IOException {
        Path tempDir = new Path(tempDataDirString);
        fs.delete(tempDir, true);
        LOGGER.info("Temp data directory '" + tempDataDirString + "' has been deleted.");
        while (fs.listStatus(tempDir.getParent()).length == 0) {
            tempDir = tempDir.getParent();
            LOGGER.info("Empty directory '" + tempDataDirString + "' has been deleted.");
            fs.delete(tempDir, true);
        }
    }
}
