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

import org.apache.commons.lang3.StringUtils;
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
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetProperties;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.AggregateGafferRowsFunction;
import uk.gov.gchq.gaffer.parquetstore.utils.ExtractKeyFromRow;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BinaryOperator;

public class AggregateAndSortGroup implements Callable<OperationException>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateAndSortGroup.class);
    private static final long serialVersionUID = -7828247145178905841L;
    private static final String SORTED = "/sorted";
    private final String group;
    private final String tempFileDir;
    private final String column;
    private final SparkSession spark;
    private final Map<String, String[]> columnToPaths;
    private final StructType sparkSchema;
    private final GafferGroupObjectConverter gafferGroupObjectConverter;
    private final String currentGraphDir;
    private final String inputDir;
    private final String outputDir;
    private final int filesPerGroup;
    private final Boolean isEntity;
    private final HashMap<String, String> propertyToAggregatorMap;
    private final Set<String> groupByColumns;
    private final String[] gafferProperties;


    public AggregateAndSortGroup(final String group,
                                 final String column,
                                 final ParquetProperties parquetStoreProperties,
                                 final String currentGraphDir,
                                 final SchemaUtils schemaUtils,
                                 final SparkSession spark) throws SerialisationException {
        this.group = group;
        this.column = column;
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
        this.gafferGroupObjectConverter = schemaUtils.getConverter(group);
        this.currentGraphDir = currentGraphDir;
        if (isEntity) {
            this.inputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, this.tempFileDir);
            this.outputDir = ParquetStore.getGroupDirectory(group, column, this.tempFileDir + SORTED);
        } else {
            this.inputDir = ParquetStore.getGroupDirectory(group, ParquetStoreConstants.SOURCE, this.tempFileDir);
            this.outputDir = ParquetStore.getGroupDirectory(group, column, this.tempFileDir + SORTED);
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
            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(new Path(inputDir))) {
                final List<String> paths = new ArrayList<>();
                paths.add(inputDir);
                if (currentGraphDir != null) {
                    if (fs.exists(new Path(currentGraphDir))) {
                        paths.add(currentGraphDir);
                    }
                }
                LOGGER.debug("Aggregating and sorting the data for group {} stored in directories {}",
                        group, StringUtils.join(paths, ','));
                final Dataset<Row> data = spark.read().parquet(JavaConversions.asScalaBuffer(paths));

                // Aggregate data
                final ExtractKeyFromRow keyExtractor = new ExtractKeyFromRow(groupByColumns, columnToPaths,
                        isEntity, propertyToAggregatorMap);
                final JavaPairRDD<Seq<Object>, GenericRowWithSchema> groupedData = data.javaRDD()
                        .mapToPair(row -> Tuple2$.MODULE$.apply(keyExtractor.call(row), (GenericRowWithSchema) row));
                final List<Tuple2<Seq<Object>, GenericRowWithSchema>> kvList = groupedData.take(1);
                if (0 == kvList.size()) {
                    LOGGER.debug("No data was returned in AggregateAndSortGroup for group = {}", group);
                    return null;
                }
                final Tuple2<Seq<Object>, GenericRowWithSchema> kv = kvList.get(0);
                final JavaPairRDD<Seq<Object>, GenericRowWithSchema> aggregatedDataKV;
                if (kv._1().size() == kv._2().size()) {
                    aggregatedDataKV = groupedData;
                } else {
                    final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(gafferProperties,
                            isEntity, groupByColumns, columnToPaths, propertyToAggregatorMap, gafferGroupObjectConverter);
                    aggregatedDataKV = groupedData.reduceByKey(aggregator);
                }
                final JavaRDD<Row> aggregatedData = aggregatedDataKV.values().map(genericRow -> (Row) genericRow);

                // Sort data
                Dataset<Row> sortedData;
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
                    if (ParquetStoreConstants.DESTINATION.equals(column)) {
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
                sortedData = spark
                        .createDataFrame(aggregatedData, sparkSchema)
                        .sort(firstSortColumn, groupBySeq.result());

                // Write out aggregated and sorted data
                sortedData
                        .coalesce(filesPerGroup)
                        .write()
                        .option("compression", "gzip")
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
