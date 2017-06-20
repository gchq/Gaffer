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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple2$;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.utils.AggregateGafferRowsFunction;
import uk.gov.gchq.gaffer.parquetstore.utils.ExtractKeyFromRow;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BinaryOperator;

public class AggregateAndSortGroup implements Callable<OperationException>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateAndSortGroup.class);
    private static final long serialVersionUID = -7828247145178905841L;
    private static final String SORTED = "/sorted";
    private final String group;
    private final String tempFileDir;
    private final boolean reverseEdge;
    private final SparkSession spark;
    private final HashMap<String, String[]> columnToPaths;
    private final StructType sparkSchema;
    private final GafferGroupObjectConverter gafferGroupObjectConverter;
    private final String inputDir;
    private final String outputDir;
    private final int filesPerGroup;
    private final Boolean isEntity;
    private final HashMap<String, String> propertyToAggregatorMap;
    private final Set<String> groupByColumns;
    private final String[] gafferProperties;


    public AggregateAndSortGroup(final String group,
                                 final boolean reverseEdge,
                                 final ParquetStoreProperties parquetStoreProperties,
                                 final SchemaUtils schemaUtils,
                                 final SparkSession spark) throws SerialisationException {
        this.group = group;
        this.reverseEdge = reverseEdge;
        this.tempFileDir = parquetStoreProperties.getTempFilesDir();
        final SchemaElementDefinition gafferSchema = schemaUtils.getGafferSchema().getElement(group);
        this.isEntity = gafferSchema instanceof SchemaEntityDefinition;
        this.propertyToAggregatorMap = buildColumnToAggregatorMap(gafferSchema);
        this.groupByColumns = new HashSet<>(gafferSchema.getGroupBy());
        this.gafferProperties = new String[gafferSchema.getProperties().size()];
        gafferSchema.getProperties().toArray(this.gafferProperties);
        this.spark = spark;
        this.columnToPaths = schemaUtils.getColumnToPaths(group);
        this.sparkSchema = schemaUtils.getSparkSchema(group);
        schemaUtils.getAvroSchema(group);
        this.gafferGroupObjectConverter = schemaUtils.getConverter(group);
        if (reverseEdge) {
            this.inputDir = schemaUtils.getGroupDirectory(group, ParquetStoreConstants.SOURCE, this.tempFileDir);
            this.outputDir = schemaUtils.getGroupDirectory(group, ParquetStoreConstants.DESTINATION, this.tempFileDir + SORTED);
        } else {
            this.inputDir = schemaUtils.getGroupDirectory(group, ParquetStoreConstants.VERTEX, this.tempFileDir);
            this.outputDir = schemaUtils.getGroupDirectory(group, ParquetStoreConstants.VERTEX, this.tempFileDir + SORTED);
        }
        this.filesPerGroup = parquetStoreProperties.getAddElementsOutputFilesPerGroup();
    }

    private HashMap<String, String> buildColumnToAggregatorMap(final SchemaElementDefinition gafferSchema) {
        final HashMap<String, String> columnToAggregatorMap = new HashMap<>();
        for (final String column : gafferSchema.getProperties()) {
            final BinaryOperator aggregateFunction = gafferSchema.getPropertyTypeDef(column).getAggregateFunction();
            if (aggregateFunction != null) {
                columnToAggregatorMap.put(column, aggregateFunction.getClass().getCanonicalName());
            }
        }
        return columnToAggregatorMap;
    }

    @Override
    public OperationException call() {
        try {
            try (final FileSystem fs = FileSystem.get(new Configuration())) {
                if (fs.exists(new Path(this.inputDir))) {
                    LOGGER.info("Aggregating and sorting the data for group {} stored in directory {}", this.group, this.inputDir);
                    final Dataset<Row> data = this.spark.read().parquet(this.inputDir);

                    // Aggregate data
                    final ExtractKeyFromRow keyExtractor = new ExtractKeyFromRow(this.groupByColumns,
                            this.columnToPaths, this.isEntity, this.propertyToAggregatorMap);
                    final JavaPairRDD<Seq<Object>, GenericRowWithSchema> groupedData = data.javaRDD()
                            .mapToPair(row -> Tuple2$.MODULE$.apply(keyExtractor.call(row), (GenericRowWithSchema) row));
                    LOGGER.debug("The data as a key/value pair ready to aggregate, looks like:");
                    if (LOGGER.isDebugEnabled()) {
                        groupedData.take(20).forEach(row -> LOGGER.debug(row.toString()));
                    }
                    final Tuple2<Seq<Object>, GenericRowWithSchema> kv = groupedData.take(1).get(0);
                    final JavaPairRDD<Seq<Object>, GenericRowWithSchema> aggregatedDataKV;
                    if (kv._1().size() == kv._2().size()) {
                        aggregatedDataKV = groupedData;
                    } else {
                        final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(this.gafferProperties,
                                this.isEntity, this.groupByColumns,
                                this.columnToPaths, this.propertyToAggregatorMap, this.gafferGroupObjectConverter);
                        aggregatedDataKV = groupedData.reduceByKey(aggregator);
                    }
                    final JavaRDD<Row> aggregatedData = aggregatedDataKV.values().map(genericRow -> (Row) genericRow).cache();

                    // Sort data
                    Dataset<Row> sortedData;
                    final String firstSortColumn;
                    final Builder<String, Seq<String>> groupBySeq = Seq$.MODULE$.newBuilder();
                    final HashMap<String, String[]> groupPaths = this.columnToPaths;
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
                        if (reverseEdge) {
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
                        } else {
                            firstSortColumn = srcPaths[0];
                            if (srcPaths.length > 1) {
                                for (int i = 1; i < srcPaths.length; i++) {
                                    groupBySeq.$plus$eq(srcPaths[i]);
                                }
                            }
                            for (final String destPath : destPaths) {
                                groupBySeq.$plus$eq(destPath);
                            }
                            groupBySeq.$plus$eq(ParquetStoreConstants.DIRECTED);
                        }
                    }
                    for (final String propName : this.groupByColumns) {
                        final String[] paths = groupPaths.get(propName);
                        if (paths != null) {
                            for (final String path : paths) {
                                groupBySeq.$plus$eq(path);
                            }
                        } else {
                            LOGGER.debug("Property " + propName + " does not have any paths");
                        }
                    }

                    sortedData = this.spark.createDataFrame(aggregatedData, this.sparkSchema)
                            .sort(firstSortColumn, groupBySeq.result());

                    // Write out aggregated and sorted data
                    sortedData
                            .coalesce(this.filesPerGroup)
                            .write()
                            .option("compression", "gzip")
                            .parquet(this.outputDir);
                } else {
                    LOGGER.debug("Skipping the sorting and aggregation of group:" + group +
                            ", due to no data existing in the temporary files directory: " + this.tempFileDir);
                }
            }
        } catch (final IOException e) {
            return new OperationException("IOException occurred during aggregation and sorting of data", e);
        }
        return null;
    }
}
