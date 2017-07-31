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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.WriteUnsortedData;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.Serializable;

/**
 * This is a function used by Spark to write out a Spark RDD's partition of data to file.
 */
public class WriteUnsortedDataFunction extends AbstractFunction1<Iterator<Element>, BoxedUnit> implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteUnsortedDataFunction.class);
    private static final long serialVersionUID = 1420859039414174311L;
    private final String tempFilesDir;
    private final byte[] gafferSchema;


    public WriteUnsortedDataFunction(final String tempFilesDir, final SchemaUtils schemaUtils) {
        this.tempFilesDir = tempFilesDir;
        this.gafferSchema = schemaUtils.getGafferSchema().toCompactJson();
    }

    @Override
    public BoxedUnit apply(final Iterator<Element> elements) {
        SchemaUtils utils = new SchemaUtils(Schema.fromJson(gafferSchema));
        final WriteUnsortedData writer = new WriteUnsortedData(tempFilesDir, utils);
        try {
            writer.writeElements(scala.collection.JavaConversions.asJavaIterator(elements));
        } catch (final OperationException e) {
            LOGGER.error("Failed to write partition: {}", e);
        }
        return null;
    }
}
