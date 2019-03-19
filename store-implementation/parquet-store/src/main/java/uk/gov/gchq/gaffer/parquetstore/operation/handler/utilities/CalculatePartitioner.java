/*
 * Copyright 2018-2019. Crown Copyright
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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.io.reader.ParquetElementReader;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GroupPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Calculates the {@link GraphPartitioner} from a snapshot directory containing all the Parquet files for a graph.
 */
public class CalculatePartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CalculatePartitioner.class);

    private final Path path;
    private final Schema schema;
    private final FileSystem fs;

    public CalculatePartitioner(final Path path, final Schema schema, final FileSystem fs) {
        this.path = path;
        this.schema = schema;
        this.fs = fs;
    }

    public GraphPartitioner call() throws IOException {
        final SchemaUtils schemaUtils = new SchemaUtils(schema);
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        for (final String group : schema.getGroups()) {
            LOGGER.info("Calculating GroupPartitioner for group {}", group);
            final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
            final List<PartitionKey> partitionKeys = new ArrayList<>();
            final Path groupPath = new Path(path, ParquetStore.getGroupSubDir(group, false));
            final FileStatus[] files = fs.listStatus(groupPath, p -> p.getName().endsWith(".parquet"));
            final SortedSet<Path> sortedFiles = new TreeSet<>();
            Arrays.stream(files).map(f -> f.getPath()).forEach(sortedFiles::add);
            final Path[] sortedPaths = sortedFiles.toArray(new Path[]{});
            LOGGER.debug("Found {} files in {}", files.length, groupPath);
            for (int i = 1; i < sortedPaths.length; i++) { // NB Skip first file
                LOGGER.debug("Reading first line of {}", sortedPaths[i]);
                final ParquetReader<Element> reader = new ParquetElementReader.Builder<Element>(sortedPaths[i])
                        .isEntity(schema.getEntityGroups().contains(group))
                        .usingConverter(converter)
                        .build();
                final Element element = reader.read(); // NB Should never be null as empty files are removed before this is called
                if (null == element) {
                    throw new IOException("No first element in file " + files[i].getPath() + " - empty files are supposed to be removed");
                }
                reader.close();
                final Object[] parquetObjects = converter.corePropertiesToParquetObjects(element);
                final PartitionKey key = new PartitionKey(parquetObjects);
                partitionKeys.add(key);
            }
            final GroupPartitioner groupPartitioner = new GroupPartitioner(group, partitionKeys);
            graphPartitioner.addGroupPartitioner(group, groupPartitioner);
            LOGGER.info("GroupPartitioner for group {} is {}", group, groupPartitioner);
        }
        for (final String group : schema.getEdgeGroups()) {
            LOGGER.info("Calculating GroupPartitioner for reversed edge group {}", group);
            final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
            final List<PartitionKey> partitionKeys = new ArrayList<>();
            final Path groupPath = new Path(path, ParquetStore.getGroupSubDir(group, true));
            final FileStatus[] files = fs.listStatus(groupPath, p -> p.getName().endsWith(".parquet"));
            final SortedSet<Path> sortedFiles = new TreeSet<>();
            Arrays.stream(files).map(f -> f.getPath()).forEach(sortedFiles::add);
            final Path[] sortedPaths = sortedFiles.toArray(new Path[]{});
            LOGGER.debug("Found {} files in {}", files.length, groupPath);
            for (int i = 1; i < sortedPaths.length; i++) { // NB Skip first file
                LOGGER.debug("Reading first line of {}", sortedPaths[i]);
                final ParquetReader<Element> reader = new ParquetElementReader.Builder<Element>(sortedPaths[i])
                        .isEntity(false)
                        .usingConverter(converter)
                        .build();
                final Edge edge = (Edge) reader.read();
                if (null == edge) {
                    throw new IOException("No first edge in file " + files[i].getPath() + " - empty files are supposed to be removed");
                }
                reader.close();
                final Object[] parquetObjects = converter.corePropertiesToParquetObjectsForReversedEdge(edge);
                final PartitionKey key = new PartitionKey(parquetObjects);
                partitionKeys.add(key);
            }
            final GroupPartitioner groupPartitioner = new GroupPartitioner(group, partitionKeys);
            graphPartitioner.addGroupPartitionerForReversedEdges(group, groupPartitioner);
        }
        return graphPartitioner;
    }
}
