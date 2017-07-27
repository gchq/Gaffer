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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class WriteUnsortedData {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteUnsortedData.class);
    private String tempFilesDir;
    private final SchemaUtils schemaUtils;
    private final Map<String, ParquetWriter<Element>> groupToWriter;
    private final Map<String, Integer> groupToFileNumber;

    public WriteUnsortedData(final ParquetStore store) {
        this(store.getTempFilesDir(), store.getSchemaUtils());
    }

    public WriteUnsortedData(final String tempFilesDir, final SchemaUtils schemaUtils) {
        this.tempFilesDir = tempFilesDir;
        this.schemaUtils = schemaUtils;
        this.groupToWriter = new HashMap<>();
        this.groupToFileNumber = new HashMap<>();
    }

    public void writeElements(final Iterator<? extends Element> elements) throws OperationException {
        try {
            // Create a writer for each group
            for (final String group : schemaUtils.getEntityGroups()) {
                groupToWriter.put(group, buildWriter(group, ParquetStoreConstants.VERTEX, true));
            }
            for (final String group : schemaUtils.getEdgeGroups()) {
                groupToWriter.put(group, buildWriter(group, ParquetStoreConstants.SOURCE, false));
            }
            // Write elements
            _writeElements(elements);
            // Close the writers
            for (final ParquetWriter<Element> writer : groupToWriter.values()) {
                writer.close();
            }
        } catch (final IOException | OperationException e) {
            throw new OperationException("Exception writing elements to temporary directory: " + tempFilesDir, e);
        }
    }

    private void _writeElements(final Iterator<? extends Element> elements) throws OperationException, IOException {
        while (elements.hasNext()) {
            final Element element = elements.next();
            final String group = element.getGroup();
            ParquetWriter<Element> writer = groupToWriter.get(group);
            if (writer != null) {
                writer.write(element);
            } else {
                LOGGER.warn("Skipped the adding of an Element with Group = {} as that group does not exist in the schema.", group);
            }
        }
    }

    private ParquetWriter<Element> buildWriter(final String group, final String column, final boolean isEntity) throws IOException {
        Integer fileNumber = groupToFileNumber.get(group);
        if (fileNumber == null) {
            groupToFileNumber.put(group, 0);
            fileNumber = 0;
        }
        LOGGER.debug("Creating a new writer for group: {}", group + " with file number " + fileNumber);
        final Path filePath = new Path(ParquetStore.getGroupDirectory(group, column,
                tempFilesDir) + "/part-" + TaskContext.getPartitionId() + "-" + fileNumber + ".parquet");

        return new ParquetElementWriter.Builder(filePath)
                .isEntity(isEntity)
                .withType(schemaUtils.getParquetSchema(group))
                .usingConverter(schemaUtils.getConverter(group))
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withSparkSchema(schemaUtils.getSparkSchema(group))
                .build();
    }
}
