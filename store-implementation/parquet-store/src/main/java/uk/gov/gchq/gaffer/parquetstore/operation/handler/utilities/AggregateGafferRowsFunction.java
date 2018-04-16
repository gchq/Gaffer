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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * This is used by the Spark reduceByKey method to aggregate two {@link GenericRowWithSchema}'s using the Gaffer aggregator's.
 */
public class AggregateGafferRowsFunction implements Function2<Row, Row, Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateGafferRowsFunction.class);
    private static final long serialVersionUID = -8353767193380574516L;
    private final Boolean isEntity;
    private final byte[] aggregatorJson;
    private final Set<String> groupByColumns;
    private final GafferGroupObjectConverter objectConverter;
    private final Map<String, String[]> columnToPaths;
    private final String[] gafferProperties;
    private transient ElementAggregator aggregator;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public AggregateGafferRowsFunction(final String[] gafferProperties,
                                       final boolean isEntity,
                                       final Set<String> groupByColumns,
                                       final Map<String, String[]> columnToPaths,
                                       final byte[] aggregatorJson,
                                       final GafferGroupObjectConverter gafferGroupObjectConverter)
            throws SerialisationException {
        LOGGER.debug("Generating a new AggregateGafferRowsFunction");
        this.gafferProperties = gafferProperties;
        this.columnToPaths = columnToPaths;
        this.objectConverter = gafferGroupObjectConverter;
        this.isEntity = isEntity;
        this.groupByColumns = groupByColumns;
        LOGGER.debug("GroupByColumns: {}", this.groupByColumns);
        this.aggregatorJson = aggregatorJson;
    }

    @Override
    public Row call(final Row v1, final Row v2)
            throws OperationException, SerialisationException {
        LOGGER.trace("First Row object to be aggregated: {}", v1);
        LOGGER.trace("Second Row object to be aggregated: {}", v2);
        ArrayList<Object> outputRow = new ArrayList<>(v1.size());
        if (isEntity) {
            for (final String col : columnToPaths.get(ParquetStoreConstants.VERTEX)) {
                outputRow.add(v1.getAs(col));
            }
        } else {
            for (final String col : columnToPaths.get(ParquetStoreConstants.SOURCE)) {
                outputRow.add(v1.getAs(col));
            }
            for (final String col : columnToPaths.get(ParquetStoreConstants.DESTINATION)) {
                outputRow.add(v1.getAs(col));
            }
            outputRow.add(v1.getAs(ParquetStoreConstants.DIRECTED));
        }

        // Build up Properties object for both rows containing just the objects that need merging
        final Properties prop1 = new Properties();
        final Properties prop2 = new Properties();
        for (final String propName : gafferProperties) {
            if (!groupByColumns.contains(propName)) {
                LOGGER.debug("Merging property: {}", propName);
                prop1.put(propName, objectConverter.sparkRowToGafferObject(propName, v1));
                prop2.put(propName, objectConverter.sparkRowToGafferObject(propName, v2));
            }
        }

        LOGGER.trace("First properties object to be aggregated: {}", prop1);
        LOGGER.trace("Second properties object to be aggregated: {}", prop2);
        // merge properties
        if (null == aggregator) {
            aggregator = JSONSerialiser.deserialise(aggregatorJson, ElementAggregator.class);
        }
        Properties mergedProperties = aggregator.apply(prop1, prop2);
        LOGGER.trace("Merged properties object after aggregation: {}", mergedProperties);

        //add properties to the row maintaining the order
        for (final String propName : gafferProperties) {
            if (groupByColumns.contains(propName)) {
                final String[] paths = columnToPaths.get(propName);
                if (paths[0].contains(".")) {
                    outputRow.add(v1.getAs(propName));
                } else {
                    for (final String column : paths) {
                        outputRow.add(v1.getAs(column));
                    }
                }
            } else {
                objectConverter.addGafferObjectToSparkRow(propName, mergedProperties.get(propName), outputRow, v1.schema());
            }
        }
        final GenericRowWithSchema mergedRow = new GenericRowWithSchema(outputRow.toArray(), v1.schema());
        LOGGER.trace("Merged row: {}", mergedRow);
        return mergedRow;
    }
}
