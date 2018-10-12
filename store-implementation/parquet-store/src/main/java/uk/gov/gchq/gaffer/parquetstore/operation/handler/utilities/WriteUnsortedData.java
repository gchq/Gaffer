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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Takes an {@link Iterable} of {@link Element}s and writes the elements out into Parquet files split into directories
 * for each group.
 */
public class WriteUnsortedData {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteUnsortedData.class);
    private String tempFilesDir;
    private final SchemaUtils schemaUtils;
    private final Map<String, Map<Integer, ParquetWriter<Element>>> groupToPartitionIdToWriter;
    private final Map<String, Map<Integer, ParquetWriter<Element>>> groupToPartitionIdToWriterForReversedEdges;
    private final GraphPartitioner graphPartitioner;
    private final BiFunction<String, Integer, String> fileNameForGroupAndPartitionId;
    private final BiFunction<String, Integer, String> fileNameForGroupAndPartitionIdForReversedEdges;

    public WriteUnsortedData(final ParquetStore store,
                             final GraphPartitioner graphPartitioner,
                             final BiFunction<String, Integer, String> fileNameForGroupAndPartitionId,
                             final BiFunction<String, Integer, String> fileNameForGroupAndPartitionIdForReversedEdges)
            throws OperationException {
        this(store.getTempFilesDir(),
                store.getSchemaUtils(),
                graphPartitioner,
                fileNameForGroupAndPartitionId,
                fileNameForGroupAndPartitionIdForReversedEdges);
    }

    public WriteUnsortedData(final String tempFilesDir,
                             final SchemaUtils schemaUtils,
                             final GraphPartitioner graphPartitioner,
                             final BiFunction<String, Integer, String> fileNameForGroupAndPartitionId,
                             final BiFunction<String, Integer, String> fileNameForGroupAndPartitionIdForReversedEdges)
            throws OperationException {
        this.tempFilesDir = tempFilesDir;
        this.schemaUtils = schemaUtils;
        this.graphPartitioner = graphPartitioner;
        this.fileNameForGroupAndPartitionId = fileNameForGroupAndPartitionId;
        this.fileNameForGroupAndPartitionIdForReversedEdges = fileNameForGroupAndPartitionIdForReversedEdges;
        this.groupToPartitionIdToWriter = new HashMap<>();
        this.groupToPartitionIdToWriterForReversedEdges = new HashMap<>();
        // Validate that graphPartitioner contains a partitioner for every group
        for (final String group : schemaUtils.getGroups()) {
            if (null == this.graphPartitioner.getGroupPartitioner(group)) {
                throw new OperationException("graphPartitioner does not contain a partitioner for group " + group);
            }
        }
        // Validate that graphPartitioner contains a partitioner for the reversed version of every edge group
        for (final String group : schemaUtils.getEdgeGroups()) {
            if (null == this.graphPartitioner.getGroupPartitionerForReversedEdges(group)) {
                throw new OperationException("graphPartitioner does not contain a partitioner for the reversed version of edge group " + group);
            }
        }
    }

    public void writeElements(final Iterable<? extends Element> elements) throws OperationException {
        // Initialise maps from groups to partition id to writers
        for (final String group : schemaUtils.getGroups()) {
            groupToPartitionIdToWriter.put(group, new HashMap<>());
        }
        for (final String group : schemaUtils.getEdgeGroups()) {
            groupToPartitionIdToWriterForReversedEdges.put(group, new HashMap<>());
        }
        try {
            // Write elements
            LOGGER.info("Writing unsorted elements");
            _writeElements(elements);
            LOGGER.info("Finished writing unsorted elements");
        } catch (final IOException e) {
            throw new OperationException("Exception writing elements to temporary directory: " + tempFilesDir, e);
        } finally {
            // Close the writers
            try {
                for (final Map<Integer, ParquetWriter<Element>> splitToWriter : groupToPartitionIdToWriter.values()) {
                    closeWriters(splitToWriter);
                }
                for (final Map<Integer, ParquetWriter<Element>> splitToWriter : groupToPartitionIdToWriterForReversedEdges.values()) {
                    closeWriters(splitToWriter);
                }
            } catch (final IOException e) {
                throw new OperationException("IOException closing writers", e);
            }
        }
    }

    private void closeWriters(final Map<Integer, ParquetWriter<Element>> writers) throws IOException {
        for (final ParquetWriter<Element> writer : writers.values()) {
            LOGGER.debug("Closing writer {}", writer);
            writer.close();
        }
    }

    private void _writeElements(final Iterable<? extends Element> elements) throws IOException {
        for (final Element element : elements) {
            if (!schemaUtils.getGroups().contains(element.getGroup())) {
                LOGGER.warn("Skipped the addition of an Element of group {} as that group does not exist in the schema.",
                        element.getGroup());
                continue;
            }
            writeElement(element);
            if (element instanceof Edge) {
                final Edge edge = (Edge) element;
                if (!edge.getSource().equals(edge.getDestination())) {
                    writeEdgeReversed((Edge) element);
                }
            }
        }
        if (elements instanceof CloseableIterable) {
            ((CloseableIterable<? extends Element>) elements).close();
        }
    }

    private void writeElement(final Element element) throws IOException {
        final String group = element.getGroup();
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        // Get partition
        final PartitionKey partitionKey = new PartitionKey(converter.corePropertiesToParquetObjects(element));
        final int partition = graphPartitioner.getGroupPartitioner(group).getPartitionId(partitionKey);
        // Get writer
        final ParquetWriter<Element> writer = getWriter(partition, group, false);
        if (null != writer) {
            writer.write(element);
        } else {
            LOGGER.warn("Skipped the addition of an Element of group {} as that group does not exist in the schema.", group);
        }
    }

    private void writeEdgeReversed(final Edge edge) throws IOException {
        // Also write out edges partitioned as in the directory sorted by destination
        final String group = edge.getGroup();
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        // Get partition
        final PartitionKey partitionKey = new PartitionKey(converter.corePropertiesToParquetObjectsForReversedEdge(edge));
        final int partition = graphPartitioner.getGroupPartitionerForReversedEdges(group).getPartitionId(partitionKey);
        // Get writer
        final ParquetWriter<Element> writer = getWriter(partition, group, true);
        if (null != writer) {
            writer.write(edge);
        } else {
            LOGGER.warn("Skipped the addition of an Element of group {} as that group does not exist in the schema.", group);
        }
    }


    private ParquetWriter<Element> getWriter(final int partition,
                                             final String group,
                                             final boolean reversed) throws IOException {
        ParquetWriter<Element> writer;
        if (!reversed) {
            writer = groupToPartitionIdToWriter.get(group).get(partition);
            if (null == writer) {
                writer = buildWriter(group, reversed, partition);
            }
            groupToPartitionIdToWriter.get(group).put(partition, writer);
        } else {
            writer = groupToPartitionIdToWriterForReversedEdges.get(group).get(partition);
            if (null == writer) {
                writer = buildWriter(group, reversed, partition);
            }
            groupToPartitionIdToWriterForReversedEdges.get(group).put(partition, writer);
        }
        return writer;
    }

    private ParquetWriter<Element> buildWriter(final String group,
                                               final boolean reversed,
                                               final Integer partitionId) throws IOException {
        final String filename;
        if (!reversed) {
            filename = fileNameForGroupAndPartitionId.apply(group, partitionId)
                    + "/part-" + TaskContext.getPartitionId() + ".parquet";
        } else {
            filename = fileNameForGroupAndPartitionIdForReversedEdges.apply(group, partitionId)
                    + "/part-" + TaskContext.getPartitionId() + ".parquet";
        }
        final Path writerPath = new Path(filename);
        LOGGER.info("Creating a new writer for group {}, partition id {} in path {}", group, partitionId, writerPath);
        return new ParquetElementWriter.Builder(writerPath)
                .withType(schemaUtils.getParquetSchema(group))
                .usingConverter(schemaUtils.getConverter(group))
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withSparkSchema(schemaUtils.getSparkSchema(group))
                .build();
    }
}
