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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Used to duplicate the edge groups that have already been aggregated and sorted by Source so that there is a copy sorted by Destination
 */
public class GenerateIndexedColumnSortedData implements Callable<OperationException>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.AggregateAndSortGroup.class);
    private static final long serialVersionUID = -7828247145178905841L;
    private final String group;
    private final SparkSession spark;
    private final Map<String, String[]> columnToPaths;
    private final String inputDir;
    private final String outputDir;
    private final int filesPerGroup;


    public GenerateIndexedColumnSortedData(final String group,
                                           final ParquetStore store,
                                           final SparkSession spark) throws SerialisationException {
        this.group = group;
        String sortedFileDir = store.getTempFilesDir() + "/" + ParquetStoreConstants.SORTED;
        this.spark = spark;
        this.columnToPaths = store.getSchemaUtils().getColumnToPaths(group);
        this.inputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.SOURCE, sortedFileDir);
        this.outputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.DESTINATION, sortedFileDir);
        this.filesPerGroup = store.getProperties().getAddElementsOutputFilesPerGroup();
    }

    @Override
    public OperationException call() {
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(new Path(inputDir))) {
                LOGGER.debug("Sorting the data for group {} to be sorted by destination using the data in {}",
                        group, inputDir);
                // Build up the list of columns to sort by
                final String firstSortColumn;
                final Builder<String, Seq<String>> groupBySeq = Seq$.MODULE$.newBuilder();
                final Map<String, String[]> groupPaths = columnToPaths;
                final String[] srcPaths = groupPaths.get(ParquetStoreConstants.SOURCE);
                final String[] destPaths = groupPaths.get(ParquetStoreConstants.DESTINATION);
                firstSortColumn = destPaths[0];
                if (destPaths.length > 1) {
                    for (int i = 1; i < destPaths.length; i++) {
                        groupBySeq.$plus$eq(destPaths[i]);
                    }
                }
                for (final String srcPath : srcPaths) {
                    groupBySeq.$plus$eq(srcPath);
                }
                groupBySeq.$plus$eq(ParquetStoreConstants.DIRECTED);

                // Write out sorted data
                spark.read()
                    .parquet(inputDir)
                    .sort(firstSortColumn, groupBySeq.result())
                    .coalesce(filesPerGroup)
                    .write()
                    .option("compression", "gzip")
                    .parquet(outputDir);
            } else {
                LOGGER.debug("Skipping the sorting of group: {} by Destination, due to no data existing in the temporary files directory: {}", inputDir);
            }
        } catch (final IOException e) {
            return new OperationException("IOException occurred during sorting of data", e);
        }
        return null;
    }
}
