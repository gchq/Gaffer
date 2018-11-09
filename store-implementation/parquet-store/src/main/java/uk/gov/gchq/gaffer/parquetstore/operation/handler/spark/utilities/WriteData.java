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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Writes an {@link Iterator} of {@link Element}s to Parquet files split by group.
 */
public class WriteData implements VoidFunction<Iterator<Element>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteData.class);

    private final HashMap<String, String> groupToDirectory; // NB Specify as HashMap, not Map, as needs to be Serializable
    private final byte[] schemaAsJson;

    public WriteData(final Function<String, String> groupToDirectory, final Schema schema) {
        this.groupToDirectory = new HashMap<>();
        for (final String group : schema.getGroups()) {
            this.groupToDirectory.put(group, groupToDirectory.apply(group));
        }
        this.schemaAsJson = schema.toCompactJson();
    }

    @Override
    public void call(final Iterator<Element> elements) throws Exception {
        final SchemaUtils schemaUtils = new SchemaUtils(Schema.fromJson(schemaAsJson));
        final int partitionId = TaskContext.getPartitionId();
        final long taskAttemptId = TaskContext.get().taskAttemptId();
        final Map<String, ParquetWriter<Element>> groupToWriter = new HashMap<>();
        final Map<String, Path> groupToWriterPath = new HashMap<>();

        for (final String group : schemaUtils.getGroups()) {
            groupToWriterPath.put(group, new Path(groupToDirectory.get(group) + "/input-" + partitionId + "-" + taskAttemptId + ".parquet"));
            groupToWriter.put(group, buildWriter(group, groupToWriterPath.get(group), schemaUtils));
        }
        while (elements.hasNext()) {
            final Element element = elements.next();
            final String group = element.getGroup();
            groupToWriter.get(group).write(element);
        }
        LOGGER.info("Finished writing elements for partition id {} and task attempt id {} to {}",
                partitionId, taskAttemptId, groupToDirectory);
        for (final ParquetWriter<Element> writer : groupToWriter.values()) {
            LOGGER.debug("Closing writer {}", writer);
            writer.close();
        }

        LOGGER.info("Renaming output files from {} to {}",
                "input-" + partitionId + "-" + taskAttemptId + ".parquet", "input-" + partitionId);
        final FileContext fileContext = FileContext.getFileContext(new Configuration());
        for (final String group : schemaUtils.getGroups()) {
            final Path src = groupToWriterPath.get(group);
            final String newName = "input-" + partitionId + ".parquet";
            final Path dst = new Path(groupToDirectory.get(group) + "/" + newName);
            try {
                fileContext.rename(src, dst);
                LOGGER.debug("Renamed {} to {}", src, dst);
            } catch (final FileAlreadyExistsException e) {
                // Another task got there first
                LOGGER.debug("Not renaming {} to {} as the destination already exists", src, dst);
            }
        }
    }

    private ParquetWriter<Element> buildWriter(final String group,
                                               final Path writerPath,
                                               final SchemaUtils schemaUtils)
            throws IOException {
        LOGGER.info("Creating a new writer for group {} at path {}", group, writerPath);
        return new ParquetElementWriter.Builder(writerPath)
                .withType(schemaUtils.getParquetSchema(group))
                .usingConverter(schemaUtils.getConverter(group))
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withSparkSchema(schemaUtils.getSparkSchema(group))
                .build();
    }
}
