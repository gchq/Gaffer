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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.utilities;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.WriteUnsortedData;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Iterator;
import java.util.Map;

/**
 * This is a function used by Spark to write out a Spark RDD's partition of data to file.
 */
public class WriteUnsortedDataFunction implements VoidFunction<Iterator<Element>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteUnsortedDataFunction.class);
    private static final long serialVersionUID = 1420859039414174311L;
    private final String tempFilesDir;
    private final byte[] gafferSchema;
    private final Map<String, Map<Object, Integer>> groupToSplitPoints;


    public WriteUnsortedDataFunction(final String tempFilesDir, final SchemaUtils schemaUtils,
                                     final Map<String, Map<Object, Integer>> groupToSplitPoints) {
        this.tempFilesDir = tempFilesDir;
        this.gafferSchema = schemaUtils.getGafferSchema().toCompactJson();
        this.groupToSplitPoints = groupToSplitPoints;
    }

    @Override
    public void call(final Iterator<Element> elements) throws Exception {
        SchemaUtils utils = new SchemaUtils(Schema.fromJson(gafferSchema));
        final WriteUnsortedData writer = new WriteUnsortedData(tempFilesDir, utils, groupToSplitPoints);
        try {
            writer.writeElements(elements);
        } catch (final OperationException e) {
            LOGGER.error("Failed to write partition: {}", e);
        }
    }
}
