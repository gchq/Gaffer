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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class WriteUnsortedData {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteUnsortedData.class);
    private final ParquetStoreProperties props;
    private final Map<String, ParquetWriter<GenericRecord>> groupToWriter;
    private final Map<String, Integer> groupToFileNumber;
    private final SchemaUtils schemaUtils;
    private final long batchSize;

    public WriteUnsortedData(final ParquetStoreProperties parquetStoreProperties, final SchemaUtils schemaUtils) {
        this.props = parquetStoreProperties;
        this.groupToWriter = new HashMap<>();
        this.groupToFileNumber = new HashMap<>();
        this.schemaUtils = schemaUtils;
        this.batchSize = parquetStoreProperties.getAddElementsBatchSize();
    }

    public void writeElements(final Iterator<? extends Element> elements) throws OperationException {
        try {
            // Create a writer for each group
            for (final String group : schemaUtils.getGroups()) {
                groupToWriter.put(group, buildWriter(group));
            }
            // Write elements
            _writeElements(elements);
            // Close the writers
            for (final ParquetWriter<GenericRecord> writer : groupToWriter.values()) {
                writer.close();
            }
        } catch (final IOException | OperationException e) {
            throw new OperationException("Exception writing elements to " + props.getTempFilesDir(), e);
        }
    }

    private void _writeElements(final Iterator<? extends Element> elements) throws OperationException, IOException {
        while (elements.hasNext()) {
            final Element element = elements.next();
            final String group = element.getGroup();
            ParquetWriter<GenericRecord> writer = groupToWriter.get(group);
            if (writer.getDataSize() >= batchSize) {
                groupToFileNumber.put(group, groupToFileNumber.getOrDefault(group, 0) + 1);
                writer.close();
                writer = buildWriter(group);
                groupToWriter.put(group, writer);
            }
            writer.write(convertElementToGenericRecord(element));
        }
    }

    private ParquetWriter<GenericRecord> buildWriter(final String group) throws IOException {
        Integer fileNumber = groupToFileNumber.get(group);
        if (fileNumber == null) {
            groupToFileNumber.put(group, 0);
            fileNumber = 0;
        }
        LOGGER.debug("Creating a new writer for group: {}", group + " with file number " + fileNumber);
        Path filePath;
        filePath = new Path(ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, props.getTempFilesDir()) +
                "/part-" + TaskContext.getPartitionId() + "-" + fileNumber + ".gz.parquet");
        return AvroParquetWriter
                .<GenericRecord>builder(filePath)
                .withSchema(schemaUtils.getAvroSchema(group))
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withRowGroupSize(props.getRowGroupSize())
                .withPageSize(props.getPageSize())
                .withDictionaryPageSize(props.getPageSize())
                .enableDictionaryEncoding()
                .build();
    }

    private GenericRecord convertElementToGenericRecord(final Element e) throws OperationException, SerialisationException {
        final String group = e.getGroup();
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schemaUtils.getAvroSchema(group));
        recordBuilder.set(ParquetStoreConstants.GROUP, group);
        if (e instanceof Entity) {
            converter.addGafferObjectToGenericRecord(ParquetStoreConstants.VERTEX, ((Entity) e).getVertex(), recordBuilder);
        } else {
            converter.addGafferObjectToGenericRecord(ParquetStoreConstants.SOURCE, ((Edge) e).getSource(), recordBuilder);
            converter.addGafferObjectToGenericRecord(ParquetStoreConstants.DESTINATION, ((Edge) e).getDestination(), recordBuilder);
            converter.addGafferObjectToGenericRecord(ParquetStoreConstants.DIRECTED, ((Edge) e).isDirected(), recordBuilder);
        }
        for (final Map.Entry<String, Object> property : e.getProperties().entrySet()) {
            converter.addGafferObjectToGenericRecord(property.getKey(), property.getValue(), recordBuilder);
        }
        return recordBuilder.build();
    }
}
