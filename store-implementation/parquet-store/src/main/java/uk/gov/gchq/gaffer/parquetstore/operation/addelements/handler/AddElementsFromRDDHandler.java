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

import org.apache.hadoop.conf.Configuration;
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
import uk.gov.gchq.gaffer.parquetstore.utils.WriteUnsortedDataFunction;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;

/**
 *
 */
public class AddElementsFromRDDHandler implements OperationHandler<ImportRDDOfElements> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromRDDHandler.class);
    @Override
    public Void doOperation(final ImportRDDOfElements operation, final Context context, final Store store) throws OperationException {
        return addElementsFromRDD(operation, context, (ParquetStore) store);
    }

    private Void addElementsFromRDD(final ImportRDDOfElements operation, final Context context, final ParquetStore store) throws OperationException {
        try {
            FileSystem fs = store.getFS();
            Path tempDir = new Path(store.getProperties().getTempFilesDir());
            if (fs.exists(tempDir)) {
                LOGGER.warn("Temp data directory '" + store.getProperties().getTempFilesDir() + "' has been deleted.");
                fs.delete(tempDir, true);
            }
            if (store.getCurrentSnapshot() != 0L) {
                LOGGER.debug("Copying data directory '" + store.getProperties().getDataDir() + "/" + store.getCurrentSnapshot() + "' has been copied to " + store.getProperties().getTempFilesDir());
                FileUtil.copy(fs, new Path(store.getProperties().getDataDir() + "/" + store.getCurrentSnapshot()), fs, tempDir, false, false, fs.getConf());

            }
            final User user = context.getUser();
            if (user instanceof SparkUser) {
                final SparkSession spark = ((SparkUser) user).getSparkSession();
                configureSpark(spark, store.getProperties());
                // Write the data out
                LOGGER.info("Starting to write the unsorted Parquet data to " + store.getProperties().getTempFilesDir() + " split by group");
                final WriteUnsortedDataFunction writeUnsortedDataFunction = new WriteUnsortedDataFunction(store.getSchemaUtils(), store.getProperties());
                operation.getInput().foreachPartition(writeUnsortedDataFunction);
                LOGGER.info("Finished writing the unsorted Parquet data to " + store.getProperties().getTempFilesDir());
                // Spark read in the data, aggregate and sort the data
                LOGGER.info("Starting to write the sorted and aggregated Parquet data to " + store.getProperties().getDataDir() + " split by group");
                new AggregateAndSortTempData(store, spark);
                LOGGER.info("Finished writing the sorted and aggregated Parquet data to " + store.getProperties().getDataDir());
                // Generate the file based index
                LOGGER.info("Starting to write the indexes");
                new GenerateIndices(store);
                LOGGER.info("Finished writing the indexes");
            } else {
                throw new OperationException("This operation requires the user to be of type SparkUser.");
            }
        } catch (IOException e) {
            throw new OperationException("IO Exception: Failed to connect to the file system", e);
        } catch (StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
        try {
            final FileSystem fs = store.getFS();
            Path tempDir = new Path(store.getProperties().getTempFilesDir());
            // move data from temp to data
            final long snapshot = System.currentTimeMillis();
            final String destPath = store.getProperties().getDataDir() + "/" + snapshot + "/";
            fs.mkdirs(new Path(destPath));
            fs.rename(new Path(store.getProperties().getTempFilesDir() + "/sorted/graph"), new Path(destPath + "graph"));
            final Path tempReversePath = new Path(store.getProperties().getTempFilesDir() + "/sorted/reverseEdges");
            if (fs.exists(tempReversePath)) {
                fs.rename(tempReversePath, new Path(destPath + "reverseEdges"));
            }
            // set the data dir property
            store.setCurrentSnapshot(snapshot);
            // reload indices
            store.loadIndices();
            fs.delete(tempDir, true);
            LOGGER.info("Temp data directory '" + tempDir.toString() + "' has been deleted.");
            while (fs.listStatus(tempDir.getParent()).length == 0) {
                tempDir = tempDir.getParent();
                LOGGER.info("Empty directory '" + tempDir.toString() + "' has been deleted.");
                fs.delete(tempDir, true);
            }
        } catch (StoreException e) {
            throw new OperationException("Failed to reload the indices", e);
        } catch (IOException e) {
            throw new OperationException("Failed to move data from temporary files directory to the data directory.", e);
        }
        return null;
    }

    private void configureSpark(final SparkSession spark, final ParquetStoreProperties props) {
        final Integer numberOfOutputFiles = props.getAddElementsOutputFilesPerGroup();
        if (numberOfOutputFiles > Integer.parseInt(spark.conf().get("spark.sql.shuffle.partitions", "200"))) {
            LOGGER.info("Setting the number of Spark shuffle partitions to " + numberOfOutputFiles);
            spark.conf().set("spark.sql.shuffle.partitions", numberOfOutputFiles);
        }

        LOGGER.info("Setting the parquet file properties");
        LOGGER.info("Row group size: " + props.getRowGroupSize());
        LOGGER.info("Page size: " + props.getPageSize());
        final Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.setInt("parquet.block.size", props.getRowGroupSize());
        hadoopConf.setInt("parquet.page.size", props.getPageSize());
        hadoopConf.setInt("parquet.dictionary.page.size", props.getPageSize());
        hadoopConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        hadoopConf.set("parquet.enable.summary-metadata", "false");
    }
}
