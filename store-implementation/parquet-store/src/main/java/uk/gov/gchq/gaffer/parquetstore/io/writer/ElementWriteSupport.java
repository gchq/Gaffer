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
package uk.gov.gchq.gaffer.parquetstore.io.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;

import java.util.HashMap;

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class ElementWriteSupport extends WriteSupport<Element> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementWriteSupport.class);
    private boolean isEntity;
    private GafferGroupObjectConverter converter;

    public static final String PARQUET_SCHEMA = "parquet.schema";

    public static MessageType getSchema(final Configuration configuration) {
        return parseMessageType(checkNotNull(configuration.get(PARQUET_SCHEMA), PARQUET_SCHEMA));
    }

    private MessageType schema = null;
    private ElementWriter elementWriter;

    public ElementWriteSupport() {
    }

    ElementWriteSupport(final MessageType schema, final boolean isEntity, final GafferGroupObjectConverter converter) {
        this.schema = schema;
        this.isEntity = isEntity;
        this.converter = converter;
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(final Configuration configuration) {
        // if present, prefer the schema passed to the constructor
        if (schema == null) {
            schema = getSchema(configuration);
        }
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        elementWriter = new ElementWriter(recordConsumer, schema, converter);
    }

    @Override
    public void write(final Element record) {
        try {
            if (isEntity) {
                elementWriter.write((Entity) record);
            } else {
                elementWriter.write((Edge) record);
            }
        } catch (SerialisationException e) {
            LOGGER.warn(e.getMessage());
        }
    }
}
