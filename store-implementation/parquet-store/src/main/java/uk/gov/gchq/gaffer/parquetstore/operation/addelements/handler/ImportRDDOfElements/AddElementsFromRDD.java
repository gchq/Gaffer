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
package uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.ImportRDDOfElements;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.AggregateAndSortTempData;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.GenerateIndices;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.RDD.AggregateGafferElements;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.RDD.ExtractKeyFromElements;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.RDD.WriteUnsortedDataFunction;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SparkParquetUtils;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;

/**
 * Executes the process of importing a {@link JavaRDD} of {@link Element} into the current {@link ParquetStore}
 */
public class AddElementsFromRDD {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromRDD.class);

    protected void addElementsFromRDD(final JavaRDD<Element> input, final Context context, final ParquetStore store)
            throws OperationException {
        try {
            final FileSystem fs = store.getFS();
            final ParquetStoreProperties parquetStoreProperties = store.getProperties();
            final String tempDataDirString = store.getTempFilesDir();
            final Path tempDir = new Path(tempDataDirString);
            final String rootDataDirString = store.getDataDir();
            if (fs.exists(tempDir)) {
                fs.delete(tempDir, true);
                LOGGER.warn("Temp data directory '{}' has been deleted.", tempDataDirString);
            }
            final User user = context.getUser();
            if (user instanceof SparkUser) {
                final SparkSession spark = ((SparkUser) user).getSparkSession();
                SparkParquetUtils.configureSparkForAddElements(spark, parquetStoreProperties);

                // aggregate new data and write out as unsorted data
                LOGGER.debug("Starting to write the new unsorted Parquet data after aggregation to {} split by group", tempDataDirString);
                final Schema gafferSchema = store.getSchema();
                boolean aggregate = true;
                for (final String group : gafferSchema.getGroups()) {
                    if (!gafferSchema.getElement(group).isAggregate()) {
                        aggregate = false;
                    }
                }
                final WriteUnsortedDataFunction writeUnsortedDataFunction =
                        new WriteUnsortedDataFunction(store.getTempFilesDir(), store.getSchemaUtils());
                if (aggregate) {
                    final ExtractKeyFromElements extractKeyFromElements = new ExtractKeyFromElements(gafferSchema);
                    final AggregateGafferElements aggregateGafferProperties = new AggregateGafferElements(gafferSchema);
                    input
                        .mapToPair(extractKeyFromElements)
                        .reduceByKey(aggregateGafferProperties)
                        .values()
                        .foreachPartition(writeUnsortedDataFunction);
                } else {
                   input
                        .foreachPartition(writeUnsortedDataFunction);
                }
                LOGGER.debug("Finished writing the unsorted Parquet data to {}", tempDataDirString);

                // Aggregate and sort data
                LOGGER.debug("Starting to write the sorted and aggregated Parquet data to {} split by group", tempDataDirString);
                new AggregateAndSortTempData(store, spark);
                LOGGER.debug("Finished writing the sorted and aggregated Parquet data to {}", tempDataDirString);

                // Generate the file based index
                LOGGER.debug("Starting to write the indexes");
                final GraphIndex newGraphIndex = new GenerateIndices(store).getGraphIndex();
                LOGGER.debug("Finished writing the indexes");
                try {
                    moveDataToDataDir(store, fs, rootDataDirString, tempDataDirString, newGraphIndex);
                    tidyUp(fs, tempDataDirString);
                } catch (final StoreException e) {
                    throw new OperationException("Failed to reload the indices", e);
                } catch (final IOException e) {
                    throw new OperationException("Failed to move data from temporary files directory to the data directory.", e);
                }
            } else {
                throw new OperationException("This operation requires the user to be of type SparkUser.");
            }
        } catch (final IOException e) {
            throw new OperationException("IOException: Failed to connect to the file system", e);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    private void moveDataToDataDir(final ParquetStore store,
                                   final FileSystem fs,
                                   final String rootDataDirString,
                                   final String tempDataDirString,
                                   final GraphIndex newGraphIndex) throws StoreException, IOException {
        // Move data from temp to data
        final long snapshot = System.currentTimeMillis();
        final String destPath = rootDataDirString + "/" + snapshot;
        fs.mkdirs(new Path(destPath).getParent());
        fs.rename(new Path(tempDataDirString + "/" + ParquetStoreConstants.SORTED), new Path(destPath));
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
