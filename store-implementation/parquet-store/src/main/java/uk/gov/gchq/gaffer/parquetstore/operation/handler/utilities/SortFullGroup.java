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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is used to sort the data for a single group by loading in the /aggregate folder within a group
 */
public class SortFullGroup implements Callable<OperationException> {

    private static final String AGGREGATED = "/aggregated";
    private static final String SORTED = "/sorted";
    private static final String SPLIT = "/split*";
    private final String inputDir;
    private final String outputDir;
    private final Map<String, String[]> columnToPaths;
    private final boolean isEntity;
    private final SparkSession spark;
    private final String column;
    private final int numberOfOutputFiles;

    public SortFullGroup(final String group,
                         final String column,
                         final ParquetStore store,
                         final SparkSession spark,
                         final int numberOfOutputFiles) throws SerialisationException {
        final String tempFileDir = store.getTempFilesDir();
        this.numberOfOutputFiles = numberOfOutputFiles;
        this.column = column;
        final SchemaElementDefinition groupGafferSchema = store.getSchemaUtils().getGafferSchema().getElement(group);
        this.isEntity = groupGafferSchema instanceof SchemaEntityDefinition;
        this.spark = spark;
        this.columnToPaths = store.getSchemaUtils().getColumnToPaths(group);
        if (isEntity) {
            this.inputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, tempFileDir) + AGGREGATED + SPLIT;
            this.outputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, tempFileDir + SORTED);
        } else {
            this.inputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.SOURCE, tempFileDir) + AGGREGATED + SPLIT;
            this.outputDir = ParquetStore.getGroupDirectory(group, column, tempFileDir + SORTED);
        }
    }

    @Override
    public OperationException call() {
        try {
            // Sort data
            final String firstSortColumn;
            final Builder<String, Seq<String>> groupBySeq = Seq$.MODULE$.newBuilder();
            final Map<String, String[]> groupPaths = columnToPaths;
            if (isEntity) {
                final String[] vertexPaths = groupPaths.get(ParquetStoreConstants.VERTEX);
                firstSortColumn = vertexPaths[0];
                if (vertexPaths.length > 1) {
                    for (int i = 1; i < vertexPaths.length; i++) {
                        groupBySeq.$plus$eq(vertexPaths[i]);
                    }
                }
            } else {
                final String[] srcPaths = groupPaths.get(ParquetStoreConstants.SOURCE);
                final String[] destPaths = groupPaths.get(ParquetStoreConstants.DESTINATION);
                if (ParquetStoreConstants.SOURCE.equals(column)) {
                    firstSortColumn = srcPaths[0];
                    if (srcPaths.length > 1) {
                        for (int i = 1; i < srcPaths.length; i++) {
                            groupBySeq.$plus$eq(srcPaths[i]);
                        }
                    }
                    for (final String destPath : destPaths) {
                        groupBySeq.$plus$eq(destPath);
                    }
                } else {
                    firstSortColumn = destPaths[0];
                    if (destPaths.length > 1) {
                        for (int i = 1; i < destPaths.length; i++) {
                            groupBySeq.$plus$eq(destPaths[i]);
                        }
                    }
                    for (final String srcPath : srcPaths) {
                        groupBySeq.$plus$eq(srcPath);
                    }
                }
                groupBySeq.$plus$eq(ParquetStoreConstants.DIRECTED);
            }

            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(new Path(inputDir).getParent())) {
                spark.read()
                        .option("mergeSchema", true)
                        .parquet(inputDir)
                        .sort(firstSortColumn, groupBySeq.result())
                        .coalesce(numberOfOutputFiles)
                        .write()
                        .option("compression", "gzip")
                        .parquet(outputDir);
            }
        } catch (final IOException e) {
            return new OperationException("IOException occurred during aggregation and sorting of data", e);
        }
        return null;
    }
}
