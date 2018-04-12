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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Aggregates and sorts a directory of parquet files that represents a single group so each group can be processed in parallel.
 */
public class AggregateGroupSplit implements Callable<OperationException>, Serializable {
    private static final String SPLIT = "/split";
    private static final String RAW = "/raw";
    private static final String AGGREGATED = "/aggregated";
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateGroupSplit.class);
    private static final long serialVersionUID = -7828247145178905841L;
    private final String group;
    private final String tempFileDir;
    private final SparkSession spark;
    private final Map<String, String[]> columnToPaths;
    private final StructType sparkSchema;
    private final GafferGroupObjectConverter gafferGroupObjectConverter;
    private final Set<String> currentGraphFiles;
    private final String inputDir;
    private final String outputDir;
    private final Boolean isEntity;
    private final String[] gafferProperties;
    private final byte[] aggregatorJson;
    private final Set<String> groupByColumns;
    private final boolean aggregate;

    public AggregateGroupSplit(final String group,
                               final String column,
                               final ParquetStore store,
                               final Set<String> currentGraphFiles,
                               final SparkSession spark,
                               final int splitNumber) throws SerialisationException {
        this.group = group;
        this.tempFileDir = store.getTempFilesDir();
        final Schema gafferSchema = store.getSchemaUtils().getGafferSchema();
        final SchemaElementDefinition groupGafferSchema = gafferSchema.getElement(group);
        this.isEntity = groupGafferSchema instanceof SchemaEntityDefinition;
        final String aggregateOnIngest = store.getProperties().get(ParquetStoreProperties.PARQUET_AGGREGATE_ON_INGEST, null);
        if (null == aggregateOnIngest) {
            this.aggregate = groupGafferSchema.isAggregate();
        } else {
            this.aggregate = Boolean.valueOf(aggregateOnIngest);
        }
        this.groupByColumns = new HashSet<>(AggregatorUtil.getIngestGroupBy(group, gafferSchema));
        this.aggregatorJson = JSONSerialiser.serialise(groupGafferSchema.getIngestAggregator());
        this.gafferProperties = new String[groupGafferSchema.getProperties().size()];
        groupGafferSchema.getProperties().toArray(this.gafferProperties);
        this.spark = spark;
        this.columnToPaths = store.getSchemaUtils().getColumnToPaths(group);
        this.sparkSchema = store.getSchemaUtils().getSparkSchema(group);
        this.gafferGroupObjectConverter = store.getSchemaUtils().getConverter(group);
        this.currentGraphFiles = currentGraphFiles;
        if (isEntity) {
            this.inputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, this.tempFileDir) + RAW + SPLIT + splitNumber;
            this.outputDir = ParquetStore.getGroupDirectory(group, column, this.tempFileDir) + AGGREGATED + SPLIT + splitNumber;
        } else {
            this.inputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.SOURCE, this.tempFileDir) + RAW + SPLIT + splitNumber;
            this.outputDir = ParquetStore.getGroupDirectory(group, column, this.tempFileDir) + AGGREGATED + SPLIT + splitNumber;
        }
    }

    @Override
    public OperationException call() {
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            final List<String> paths = new ArrayList<>();
            if (fs.exists(new Path(inputDir))) {
                paths.add(inputDir);
            }
            if (null != currentGraphFiles && !currentGraphFiles.isEmpty()) {
                for (final String currentGraphFile : currentGraphFiles) {
                    final Path currentGraphFilePath = new Path(currentGraphFile);
                    if (fs.exists(currentGraphFilePath.getParent())) {
                        for (final FileStatus file : fs.globStatus(currentGraphFilePath)) {
                            paths.add(file.getPath().toString());
                        }
                    }
                }
            }
            if (!paths.isEmpty()) {
                LOGGER.debug("Aggregating and sorting the data for group {} stored in directories {}",
                        group, StringUtils.join(paths, ','));
                final Dataset<Row> data = spark.read().parquet(JavaConversions.asScalaBuffer(paths));

                // Aggregate data
                final Dataset<Row> aggregatedData;
                if (aggregate) {
                    final ExtractKeyFromRow keyExtractor = new ExtractKeyFromRow(groupByColumns, columnToPaths, isEntity);
                    final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(gafferProperties,
                            isEntity, groupByColumns, columnToPaths, aggregatorJson, gafferGroupObjectConverter);
                    final JavaRDD<Row> aggregatedRDD = data.javaRDD()
                            .keyBy(keyExtractor)
                            .reduceByKey(aggregator)
                            .values();
                    aggregatedData = spark.createDataFrame(aggregatedRDD, sparkSchema);
                } else {
                    aggregatedData = data;
                }

                // Write out aggregated data
                aggregatedData
                        .write()
                        .parquet(outputDir);
            } else {
                LOGGER.debug("Skipping the sorting and aggregation of group: {}, due to no data existing in the temporary files directory: {}", group, tempFileDir);
            }
        } catch (final IOException e) {
            return new OperationException("IOException occurred during aggregation and sorting of data", e);
        }
        return null;
    }
}
