/*
 * Copyright 2018 Crown Copyright
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GroupPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CalculatePartitionerTest {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private Schema getSchema() {
        return TestUtils.gafferSchema("schemaUsingLongVertexType");
    }

    public static void writeData(final String folder, final SchemaUtils schemaUtils) throws IOException {
        // - Write 10 files for BasicEntity and BasicEntity2 groups where partition i has entries i*10, i*10 + 1, ..., i*10 + 9
        for (final String group : Arrays.asList(TestGroups.ENTITY, TestGroups.ENTITY_2)) {
            final Path groupFolderPath = new Path(folder, ParquetStore.getGroupSubDir(group, false));
            for (int partition = 0; partition < 10; partition++) {
                final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
                final ParquetWriter<Element> writer = new ParquetElementWriter.Builder(pathForPartitionFile)
                        .usingConverter(schemaUtils.getConverter(group))
                        .withType(schemaUtils.getParquetSchema(group))
                        .usingConverter(schemaUtils.getConverter(group))
                        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                        .withSparkSchema(schemaUtils.getSparkSchema(group))
                        .build();
                for (int i = 0; i < 10; i++) {
                    final Entity entity = new Entity(group, new Integer(partition * 10 + i).longValue());
                    entity.putProperty("byte", (byte) 20);
                    entity.putProperty("double", 20.0D);
                    entity.putProperty("float", 30F);
                    entity.putProperty("treeSet", TestUtils.getTreeSet1());
                    entity.putProperty("long", 100L);
                    entity.putProperty("short", (short) 40);
                    entity.putProperty("date", new Date());
                    entity.putProperty("freqMap", TestUtils.getFreqMap1());
                    entity.putProperty("count", 50);
                    writer.write(entity);
                }
                writer.close();
            }
        }
        // - Write 10 files for BasicEdge and BasicEdge2 groups where partition i has entries
        //   source = i*10, destination = i*10 + 1
        //   source = i*10 + 1, destination = i*10 + 2
        //   ...
        //   source = i*10 + 9, destination = i*10 + 10
        for (final String group : Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2)) {
            final Path groupFolderPath = new Path(folder, ParquetStore.getGroupSubDir(group, false));
            for (int partition = 0; partition < 10; partition++) {
                final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
                final ParquetWriter<Element> writer = new ParquetElementWriter.Builder(pathForPartitionFile)
                        .usingConverter(schemaUtils.getConverter(group))
                        .withType(schemaUtils.getParquetSchema(group))
                        .usingConverter(schemaUtils.getConverter(group))
                        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                        .withSparkSchema(schemaUtils.getSparkSchema(group))
                        .build();
                for (int i = 0; i < 10; i++) {
                    final Edge edge = new Edge(group,
                            new Integer(partition * 10 + i).longValue(),
                            new Integer(partition * 10 + i + 1).longValue(),
                            true);
                    edge.putProperty("byte", (byte) 20);
                    edge.putProperty("double", 20.0D);
                    edge.putProperty("float", 30F);
                    edge.putProperty("treeSet", TestUtils.getTreeSet1());
                    edge.putProperty("long", 100L);
                    edge.putProperty("short", (short) 40);
                    edge.putProperty("date", new Date());
                    edge.putProperty("freqMap", TestUtils.getFreqMap1());
                    edge.putProperty("count", 50);
                    writer.write(edge);
                }
                writer.close();
            }
        }
        // - Write 10 files for reversed edges BasicEdge and BasicEdge2 groups where partition i has entries
        //   source = i*10, destination = i*10 + 1
        //   source = i*10 + 1, destination = i*10 + 2
        //   ...
        //   source = i*10 + 9, destination = i*10 + 10
        for (final String group : Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2)) {
            final Path groupFolderPath = new Path(folder, ParquetStore.getGroupSubDir(group, true));
            for (int partition = 0; partition < 10; partition++) {
                final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
                final ParquetWriter<Element> writer = new ParquetElementWriter.Builder(pathForPartitionFile)
                        .usingConverter(schemaUtils.getConverter(group))
                        .withType(schemaUtils.getParquetSchema(group))
                        .usingConverter(schemaUtils.getConverter(group))
                        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                        .withSparkSchema(schemaUtils.getSparkSchema(group))
                        .build();
                for (int i = 0; i < 10; i++) {
                    final Edge edge = new Edge(group,
                            new Integer(partition * 10 + i).longValue(),
                            new Integer(partition * 10 + i + 1).longValue(),
                            true);
                    edge.putProperty("byte", (byte) 20);
                    edge.putProperty("double", 20.0D);
                    edge.putProperty("float", 30F);
                    edge.putProperty("treeSet", TestUtils.getTreeSet1());
                    edge.putProperty("long", 100L);
                    edge.putProperty("short", (short) 40);
                    edge.putProperty("date", new Date());
                    edge.putProperty("freqMap", TestUtils.getFreqMap1());
                    edge.putProperty("count", 50);
                    writer.write(edge);
                }
                writer.close();
            }
        }

    }

    @Test
    public void calculatePartitionerTest() throws IOException {
        // Given
        final FileSystem fs = FileSystem.get(new Configuration());
        final Schema schema = getSchema();
        final SchemaUtils schemaUtils = new SchemaUtils(schema);
        final String topLevelFolder = testFolder.newFolder().toPath().toString();
        writeData(topLevelFolder, schemaUtils);

        // When
        // - Calculate partitioner from files
        final GraphPartitioner actual = new CalculatePartitioner(new Path(topLevelFolder), schema, fs).call();
        // - Manually create the correct partitioner
        final GraphPartitioner expected = new GraphPartitioner();
        final List<PartitionKey> splitPointsEntity = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            splitPointsEntity.add(new PartitionKey(new Object[]{10L * i}));
        }
        final GroupPartitioner groupPartitionerEntity = new GroupPartitioner(TestGroups.ENTITY, splitPointsEntity);
        expected.addGroupPartitioner(TestGroups.ENTITY, groupPartitionerEntity);
        final GroupPartitioner groupPartitionerEntity2 = new GroupPartitioner(TestGroups.ENTITY_2, splitPointsEntity);
        expected.addGroupPartitioner(TestGroups.ENTITY_2, groupPartitionerEntity2);
        final List<PartitionKey> splitPointsEdge = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            splitPointsEdge.add(new PartitionKey(new Object[]{10L * i, 10L * i + 1, true}));
        }
        final GroupPartitioner groupPartitionerEdge = new GroupPartitioner(TestGroups.EDGE, splitPointsEdge);
        expected.addGroupPartitioner(TestGroups.EDGE, groupPartitionerEdge);
        final GroupPartitioner groupPartitionerEdge2 = new GroupPartitioner(TestGroups.EDGE_2, splitPointsEdge);
        expected.addGroupPartitioner(TestGroups.EDGE_2, groupPartitionerEdge2);
        final List<PartitionKey> splitPointsReversedEdge = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            splitPointsReversedEdge.add(new PartitionKey(new Object[]{10L * i + 1, 10L * i, true}));
        }
        final GroupPartitioner reversedGroupPartitionerEdge = new GroupPartitioner(TestGroups.EDGE, splitPointsReversedEdge);
        expected.addGroupPartitionerForReversedEdges(TestGroups.EDGE, reversedGroupPartitionerEdge);
        final GroupPartitioner reversedGroupPartitionerEdge2 = new GroupPartitioner(TestGroups.EDGE_2, splitPointsReversedEdge);
        expected.addGroupPartitionerForReversedEdges(TestGroups.EDGE_2, reversedGroupPartitionerEdge2);

        // Then
        assertEquals(expected, actual);
    }
}
