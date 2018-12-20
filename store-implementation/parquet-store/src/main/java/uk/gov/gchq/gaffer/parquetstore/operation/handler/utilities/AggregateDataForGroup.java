/*
 * Copyright 2018. Crown Copyright
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
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
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
 * Aggregates data in Parquet files from one group.
 */
public class AggregateDataForGroup implements Callable<CallableResult>, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateDataForGroup.class);

    private final String group;
    private final List<String> inputFiles;
    private final String outputDir;
    private final SparkSession sparkSession;
    private final Set<String> groupByColumns;
    private final Map<String, String[]> columnToPaths;
    private final boolean isEntity;
    private final String[] gafferProperties;
    private final byte[] aggregatorSerialisedToJson;
    private final GafferGroupObjectConverter gafferGroupObjectConverter;
    private final StructType sparkSchema;
    private final FileSystem fs;

    public AggregateDataForGroup(final FileSystem fs,
                                 final SchemaUtils schemaUtils,
                                 final String group,
                                 final List<String> inputFiles,
                                 final String outputDir,
                                 final SparkSession sparkSession) throws SerialisationException {
        this.fs = fs;
        this.group = group;
        this.inputFiles = inputFiles;
        this.outputDir = outputDir;
        this.sparkSession = sparkSession;
        this.groupByColumns = new HashSet<>(AggregatorUtil.getIngestGroupBy(group, schemaUtils.getGafferSchema()));
        this.columnToPaths = schemaUtils.getColumnToPaths(group);
        this.isEntity = schemaUtils.getGafferSchema().getEntityGroups().contains(group);
        final Set<String> propertiesSet = schemaUtils.getGafferSchema().getElement(group).getProperties();
        this.gafferProperties = new String[propertiesSet.size()];
        propertiesSet.toArray(this.gafferProperties);
        this.aggregatorSerialisedToJson = JSONSerialiser.serialise(schemaUtils.getGafferSchema()
                .getElement(group).getIngestAggregator());
        this.gafferGroupObjectConverter = schemaUtils.getConverter(group);
        this.sparkSchema = schemaUtils.getSparkSchema(group);
    }

    @Override
    public CallableResult call() {
        return aggregate();
    }

    private CallableResult aggregate() {
        final ExtractKeyFromRow keyExtractor = new ExtractKeyFromRow(groupByColumns, columnToPaths, isEntity, false);

        try {
            if (inputFiles.isEmpty()) {
                LOGGER.info("Not aggregating data for group {} as list of input files is empty", group);
                return null;
            }

            final List<String> inputFilesThatExist = new ArrayList<>();
            for (final String file : inputFiles) {
                if (!fs.exists(new Path(file))) {
                    LOGGER.info("Ignoring file {} as it does not exist", file);
                } else {
                    inputFilesThatExist.add(file);
                }
            }
            if (inputFilesThatExist.isEmpty()) {
                LOGGER.info("Not aggregating data for group {} as list of input files that exist is empty", group);
                return null;
            }

            final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(gafferProperties,
                    isEntity, groupByColumns, columnToPaths, aggregatorSerialisedToJson, gafferGroupObjectConverter);

            final Dataset<Row> data = sparkSession.read().parquet(JavaConversions.asScalaBuffer(inputFilesThatExist));

            final JavaRDD<Row> aggregatedRDD = data
                    .javaRDD()
                    .keyBy(keyExtractor)
                    .reduceByKey(aggregator)
                    .values();
            final Dataset<Row> aggregatedData = sparkSession.createDataFrame(aggregatedRDD, sparkSchema);

            // Write out aggregated data
            aggregatedData
                    .write()
                    .parquet(outputDir);

        } catch (final IOException e) {
            return CallableResult.FAILURE;
        }
        return CallableResult.SUCCESS;
    }
}
