/*
 * Copyright 2017-2018. Crown Copyright
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
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides the required {@link WriteSupport} to write out {@link Element}s to Parquet files. This is used to
 * pass the Spark schema into the Parquet extra metadata (which will speed up the reading of the Parquet files by Spark).
 */
public class ElementWriteSupport extends WriteSupport<Element> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementWriteSupport.class);
    private GafferGroupObjectConverter converter;
    private MessageType schema = null;
    private ElementWriter elementWriter;
    private StructType sparkSchema;

    public ElementWriteSupport() {
    }

    ElementWriteSupport(final MessageType schema,
                        final GafferGroupObjectConverter converter,
                        final StructType sparkSchema) {
        this.schema = schema;
        this.converter = converter;
        this.sparkSchema = sparkSchema;
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(final Configuration configuration) {
        final Map<String, String> extraMeta = new HashMap<>();
        if (null != sparkSchema) {
            extraMeta.put(ParquetReadSupport.SPARK_METADATA_KEY(), sparkSchema.json());
        }
        return new WriteContext(schema, extraMeta);
    }

    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        elementWriter = new ElementWriter(recordConsumer, schema, converter);
    }

    @Override
    public void write(final Element element) {
        try {
            elementWriter.writeElement(element);
        } catch (final SerialisationException e) {
            LOGGER.warn("SerialisationException whilst writing element: {}", e.getMessage());
        }
    }
}
