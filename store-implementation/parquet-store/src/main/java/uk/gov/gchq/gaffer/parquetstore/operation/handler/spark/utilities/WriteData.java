/*
 * Copyright 2018-2021 Crown Copyright
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
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link VoidFunction} that writes an {@link Iterator} of {@link Element}s to multiple Parquet files, with one file
 * per group. Each file is initially named using both the partition id and the task attempt id. This name is unique
 * across multiple simultaneous calls of this function for the same partition. This ensures that tasks that are
 * processing the same partition do not attempt to write to the same file. Once all the data from the {@link Iterator}
 * has been written, the files are closed. Each file is then renamed to input-partitionId.parquet. If a
 * {@link FileAlreadyExistsException} is thrown, this is ignored as it means that another task has already completed
 * writing this file and so the results of this task are not needed.
 */
public class WriteData implements VoidFunction<Iterator<Element>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteData.class);

    private final HashMap<String, String> groupToDirectory; // NB Specify as HashMap, not Map, as needs to be Serializable
    private final byte[] schemaAsJson;
    private final CompressionCodecName compressionCodecName;

    public WriteData(final Function<String, String> groupToDirectory,
                     final Schema schema,
                     final CompressionCodecName compressionCodecName) {
        this.groupToDirectory = new HashMap<>();
        for (final String group : schema.getGroups()) {
            this.groupToDirectory.put(group, groupToDirectory.apply(group));
        }
        this.schemaAsJson = schema.toCompactJson();
        this.compressionCodecName = compressionCodecName;
    }

    @Override
    public void call(final Iterator<Element> elements) throws Exception {
        final TaskContext taskContext = TaskContext.get();
        if (null == taskContext) {
            throw new OperationException("Method call(Iterator<Element>) should only be called from within a Spark job");
        }
        call(elements, taskContext.partitionId(), taskContext.taskAttemptId());
    }

    // Not private to allow tests to call it
    void call(final Iterator<Element> elements, final int partitionId, final long taskAttemptId) throws Exception {
        final SchemaUtils schemaUtils = new SchemaUtils(Schema.fromJson(schemaAsJson));
        final Map<String, ParquetWriter<Element>> groupToWriter = new HashMap<>();
        final Map<String, Path> groupToWriterPath = new HashMap<>();
        for (final String group : schemaUtils.getGroups()) {
            groupToWriterPath.put(group, new Path(groupToDirectory.get(group) + "/input-" + partitionId + "-" + taskAttemptId + ".parquet"));
            groupToWriter.put(group, buildWriter(group, groupToWriterPath.get(group), schemaUtils));
        }
        writeData(elements, partitionId, taskAttemptId, groupToWriter);
        renameFiles(partitionId, taskAttemptId, schemaUtils.getGroups(), groupToWriterPath);
    }

    private void writeData(final Iterator<Element> elements,
                           final int partitionId,
                           final long taskAttemptId,
                           final Map<String, ParquetWriter<Element>> groupToWriter) throws IOException {
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
    }

    private void renameFiles(final int partitionId,
                             final long taskAttemptId,
                             final Set<String> groups,
                             final Map<String, Path> groupToWriterPath) throws Exception {
        LOGGER.info("Renaming output files from {} to {}",
                "input-" + partitionId + "-" + taskAttemptId + ".parquet", "input-" + partitionId);
        final FileContext fileContext = FileContext.getFileContext(new Configuration());
        for (final String group : groups) {
            final Path src = groupToWriterPath.get(group);
            final String newName = "input-" + partitionId + ".parquet";
            final Path dst = new Path(groupToDirectory.get(group) + "/" + newName);
            try {
                fileContext.rename(src, dst, Options.Rename.NONE);
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
                .withCompressionCodec(compressionCodecName)
                .withSparkSchema(schemaUtils.getSparkSchema(group))
                .build();
    }
}
