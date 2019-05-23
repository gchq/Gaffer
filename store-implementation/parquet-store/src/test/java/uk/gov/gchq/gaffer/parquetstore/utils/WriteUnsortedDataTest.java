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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.io.reader.ParquetElementReader;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.WriteUnsortedData;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GroupPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WriteUnsortedDataTest {
    private static FileSystem fs;
    private static Date date0;
    private static Date date1;
    private static Date date2;
    private static Date date3;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            date0 = DATE_FORMAT.parse("1970-01-01 00:00:00");
            date1 = DATE_FORMAT.parse("1971-01-01 00:00:00");
            date2 = DATE_FORMAT.parse("1972-01-01 00:00:00");
            date3 = DATE_FORMAT.parse("1973-01-01 00:00:00");
        } catch (final ParseException e1) {
            // Won't happen
        }
    }

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void setUp() throws IOException {
        fs = FileSystem.get(new Configuration());
        Logger.getRootLogger().setLevel(Level.WARN);
    }

    @Test
    public void testNoSplitPointsCase() throws IOException, OperationException {
        // Given
        final String tempFilesDir = testFolder.newFolder().getAbsolutePath();
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        graphPartitioner.addGroupPartitioner(TestGroups.ENTITY, new GroupPartitioner(TestGroups.ENTITY, new ArrayList<>()));
        graphPartitioner.addGroupPartitioner(TestGroups.ENTITY_2, new GroupPartitioner(TestGroups.ENTITY_2, new ArrayList<>()));
        graphPartitioner.addGroupPartitioner(TestGroups.EDGE, new GroupPartitioner(TestGroups.EDGE, new ArrayList<>()));
        graphPartitioner.addGroupPartitioner(TestGroups.EDGE_2, new GroupPartitioner(TestGroups.EDGE_2, new ArrayList<>()));
        graphPartitioner.addGroupPartitionerForReversedEdges(TestGroups.EDGE, new GroupPartitioner(TestGroups.EDGE, new ArrayList<>()));
        graphPartitioner.addGroupPartitionerForReversedEdges(TestGroups.EDGE_2, new GroupPartitioner(TestGroups.EDGE_2, new ArrayList<>()));
        final List<Element> elements = getData(3L);
        final BiFunction<String, Integer, String> fileNameForGroupAndPartitionId = (group, partitionId) ->
                tempFilesDir + "/GROUP=" + group + "/split-" + partitionId;
        final BiFunction<String, Integer, String> fileNameForGroupAndPartitionIdForReversedEdge = (group, partitionId) ->
                tempFilesDir + "/REVERSED-GROUP=" + group + "/split-" + partitionId;
        final WriteUnsortedData writeUnsortedData = new WriteUnsortedData(tempFilesDir, CompressionCodecName.GZIP,
                schemaUtils, graphPartitioner, fileNameForGroupAndPartitionId,
                fileNameForGroupAndPartitionIdForReversedEdge);

        // When
        writeUnsortedData.writeElements(elements);

        // Then
        //  - Each directory should exist and contain one file
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY_2 + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE_2 + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE_2 + "/split-0", 1);
        //  - Each file should contain the data that was written to it, in the order it was in the iterable
        testContainsCorrectDataNoSplitPoints(TestGroups.ENTITY,
                tempFilesDir + "/GROUP=" + TestGroups.ENTITY + "/split-0",
                elements,
                schemaUtils);
        testContainsCorrectDataNoSplitPoints(TestGroups.ENTITY_2,
                tempFilesDir + "/GROUP=" + TestGroups.ENTITY_2 + "/split-0",
                elements,
                schemaUtils);
        testContainsCorrectDataNoSplitPoints(TestGroups.EDGE,
                tempFilesDir + "/GROUP=" + TestGroups.EDGE + "/split-0",
                elements,
                schemaUtils);
        testContainsCorrectDataNoSplitPoints(TestGroups.EDGE_2,
                tempFilesDir + "/GROUP=" + TestGroups.EDGE_2 + "/split-0",
                elements,
                schemaUtils);
        testContainsCorrectDataNoSplitPoints(TestGroups.EDGE,
                tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE + "/split-0",
                elements,
                schemaUtils);
        final List<Element> elementsWithSameSrcDstRemoved = elements.stream()
                .filter(e -> e.getGroup().equals(TestGroups.EDGE_2))
                .map(e -> (Edge) e)
                .filter(e -> !e.getSource().equals(e.getDestination()))
                .collect(Collectors.toList());
        testContainsCorrectDataNoSplitPoints(TestGroups.EDGE_2,
                tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE_2 + "/split-0",
                elementsWithSameSrcDstRemoved,
                schemaUtils);
    }

    @Test
    public void testOneSplitPointCase() throws IOException, OperationException {
        // Given
        final String tempFilesDir = testFolder.newFolder().getAbsolutePath();
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        final List<Element> elements = new ArrayList<>();
        // TestGroups.ENTITY, split point is 10L. Create data with
        //        VERTEX
        //          5L
        //         10L
        //         10L
        //         10L
        //         20L
        final List<PartitionKey> splitPointsEntity = new ArrayList<>();
        splitPointsEntity.add(new PartitionKey(new Object[]{10L}));
        graphPartitioner.addGroupPartitioner(TestGroups.ENTITY, new GroupPartitioner(TestGroups.ENTITY, splitPointsEntity));
        elements.add(createEntityForEntityGroup(5L));
        elements.add(createEntityForEntityGroup(10L));
        elements.add(createEntityForEntityGroup(10L));
        elements.add(createEntityForEntityGroup(10L));
        elements.add(createEntityForEntityGroup(20L));
        // TestGroups.ENTITY_2, split point is 100L. Create data with
        //        VERTEX
        //          5L
        //        100L
        //       1000L
        final List<PartitionKey> splitPointsEntity_2 = new ArrayList<>();
        splitPointsEntity_2.add(new PartitionKey(new Object[]{100L}));
        graphPartitioner.addGroupPartitioner(TestGroups.ENTITY_2, new GroupPartitioner(TestGroups.ENTITY_2, splitPointsEntity_2));
        elements.add(createEntityForEntityGroup_2(5L));
        elements.add(createEntityForEntityGroup_2(100L));
        elements.add(createEntityForEntityGroup_2(1000L));
        // TestGroups.EDGE, split point is [1000L, 200L, true]. Create data with
        //        SOURCE   DESTINATION    DIRECTED
        //          5L         5000L        true
        //          5L         200L         false
        //       1000L         100L         true
        //       1000L         200L         false
        //       1000L         200L         true
        //       1000L         300L         true
        //      10000L         400L         false
        //      10000L         400L         true
        final List<PartitionKey> splitPointsEdge = new ArrayList<>();
        splitPointsEdge.add(new PartitionKey(new Object[]{1000L, 200L, true}));
        graphPartitioner.addGroupPartitioner(TestGroups.EDGE, new GroupPartitioner(TestGroups.EDGE, splitPointsEdge));
        final List<PartitionKey> splitPointsReversedEdge = new ArrayList<>();
        splitPointsReversedEdge.add(new PartitionKey(new Object[]{1000L, 300L, true}));
        graphPartitioner.addGroupPartitionerForReversedEdges(TestGroups.EDGE, new GroupPartitioner(TestGroups.EDGE, splitPointsReversedEdge));
        elements.add(createEdgeForEdgeGroup(5L, 5000L, true));
        elements.add(createEdgeForEdgeGroup(5L, 200L, false));
        elements.add(createEdgeForEdgeGroup(1000L, 100L, true));
        elements.add(createEdgeForEdgeGroup(1000L, 200L, false));
        elements.add(createEdgeForEdgeGroup(1000L, 200L, true));
        elements.add(createEdgeForEdgeGroup(1000L, 300L, true));
        elements.add(createEdgeForEdgeGroup(10000L, 400L, false));
        elements.add(createEdgeForEdgeGroup(10000L, 400L, true));
        // TestGroups.EDGE_2, split point is [10L, 2000L, true]. Create data with
        //        SOURCE   DESTINATION    DIRECTED
        //          5L         5000L        true
        //         10L         2000L        false
        //         10L         2000L        true
        //         10L         3000L        false
        //        100L         1000L        true
        //        100L         3000L        false
        //        100L         3000L        true
        final List<PartitionKey> splitPointsEdge_2 = new ArrayList<>();
        splitPointsEdge_2.add(new PartitionKey(new Object[]{10L, 2000L, true}));
        graphPartitioner.addGroupPartitioner(TestGroups.EDGE_2, new GroupPartitioner(TestGroups.EDGE_2, splitPointsEdge_2));
        final List<PartitionKey> splitPointsReversedEdge_2 = new ArrayList<>();
        splitPointsReversedEdge_2.add(new PartitionKey(new Object[]{3000L, 20L, true}));
        graphPartitioner.addGroupPartitionerForReversedEdges(TestGroups.EDGE_2, new GroupPartitioner(TestGroups.EDGE_2, splitPointsReversedEdge_2));
        elements.add(createEdgeForEdgeGroup_2(5L, 5000L, true));
        elements.add(createEdgeForEdgeGroup_2(5L, 200L, false));
        elements.add(createEdgeForEdgeGroup_2(1000L, 100L, true));
        elements.add(createEdgeForEdgeGroup_2(1000L, 200L, false));
        elements.add(createEdgeForEdgeGroup_2(1000L, 200L, true));
        elements.add(createEdgeForEdgeGroup_2(1000L, 300L, true));
        elements.add(createEdgeForEdgeGroup_2(10000L, 400L, false));
        elements.add(createEdgeForEdgeGroup_2(10000L, 400L, true));
        final BiFunction<String, Integer, String> fileNameForGroupAndPartitionId = (group, partitionId) ->
                tempFilesDir + "/GROUP=" + group + "/split-" + partitionId;
        final BiFunction<String, Integer, String> fileNameForGroupAndPartitionIdForReversedEdge = (group, partitionId) ->
                tempFilesDir + "/REVERSED-GROUP=" + group + "/split-" + partitionId;
        final WriteUnsortedData writeUnsortedData = new WriteUnsortedData(tempFilesDir, CompressionCodecName.GZIP,
                schemaUtils, graphPartitioner, fileNameForGroupAndPartitionId,
                fileNameForGroupAndPartitionIdForReversedEdge);

        // When
        writeUnsortedData.writeElements(elements);

        // Then
        //  - For each group, directories split0 and split1 should exist and each contain one file
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY_2 + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY_2 + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE_2 + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE_2 + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE_2 + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/REVERSED-GROUP=" + TestGroups.EDGE_2 + "/split-1", 1);
        //  - Each split file should contain the data for that split in the order it was written
        for (final String group : new HashSet<>(Arrays.asList(TestGroups.ENTITY, TestGroups.ENTITY_2))) {
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-0",
                    group, true, false, null, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-1",
                    group, true, false, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    null, elements, schemaUtils);
        }
        for (final String group : new HashSet<>(Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2))) {
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-0",
                    group, false, false, null, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/REVERSED-GROUP=" + group + "/split-0",
                    group, false, true, null,
                    graphPartitioner.getGroupPartitionerForReversedEdges(group).getIthPartitionKey(0),
                    elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-1",
                    group, false, false, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    null, elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/REVERSED-GROUP=" + group + "/split-1",
                    group, false, true,
                    graphPartitioner.getGroupPartitionerForReversedEdges(group).getIthPartitionKey(0),
                    null, elements, schemaUtils);
        }
    }

    @Test
    public void testMultipleSplitPointsCase() throws IOException, OperationException {
        // Given
        final String tempFilesDir = testFolder.newFolder().getAbsolutePath();
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        final List<Element> elements = new ArrayList<>();
        // TestGroups.ENTITY, split points are 10L and 100L. Create data with
        //        VERTEX
        //          5L
        //         10L
        //         10L
        //         11L
        //         12L
        //        100L
        //        100L
        //        200L
        final List<PartitionKey> splitPointsEntity = new ArrayList<>();
        splitPointsEntity.add(new PartitionKey(new Object[]{10L}));
        splitPointsEntity.add(new PartitionKey(new Object[]{100L}));
        graphPartitioner.addGroupPartitioner(TestGroups.ENTITY, new GroupPartitioner(TestGroups.ENTITY, splitPointsEntity));
        elements.add(createEntityForEntityGroup(5L));
        elements.add(createEntityForEntityGroup(10L));
        elements.add(createEntityForEntityGroup(10L));
        elements.add(createEntityForEntityGroup(11L));
        elements.add(createEntityForEntityGroup(12L));
        elements.add(createEntityForEntityGroup(100L));
        elements.add(createEntityForEntityGroup(100L));
        elements.add(createEntityForEntityGroup(200L));
        // TestGroups.ENTITY_2, split points are 100L and 1000L. Create data with
        //        VERTEX
        //          5L
        //        100L
        //        200L
        //       1000L
        //       5000L
        final List<PartitionKey> splitPointsEntity_2 = new ArrayList<>();
        splitPointsEntity_2.add(new PartitionKey(new Object[]{100L}));
        splitPointsEntity_2.add(new PartitionKey(new Object[]{1000L}));
        graphPartitioner.addGroupPartitioner(TestGroups.ENTITY_2, new GroupPartitioner(TestGroups.ENTITY_2, splitPointsEntity_2));
        elements.add(createEntityForEntityGroup_2(5L));
        elements.add(createEntityForEntityGroup_2(100L));
        elements.add(createEntityForEntityGroup_2(200L));
        elements.add(createEntityForEntityGroup_2(1000L));
        elements.add(createEntityForEntityGroup_2(5000L));
        // TestGroups.EDGE, split points are [1000L, 200L, true] and [1000L, 30000L, false]. Create data with
        //        SOURCE   DESTINATION    DIRECTED
        //          5L        5000L         true
        //          5L         200L         false
        //       1000L         100L         true
        //       1000L       10000L         false
        //       1000L       30000L         false
        //       1000L      300000L         true
        //      10000L         400L         false
        final List<PartitionKey> splitPointsEdge = new ArrayList<>();
        splitPointsEdge.add(new PartitionKey(new Object[]{1000L, 200L, true}));
        splitPointsEdge.add(new PartitionKey(new Object[]{1000L, 30000L, false}));
        graphPartitioner.addGroupPartitioner(TestGroups.EDGE, new GroupPartitioner(TestGroups.EDGE, splitPointsEdge));
        final List<PartitionKey> splitPointsReversedEdge = new ArrayList<>();
        splitPointsReversedEdge.add(new PartitionKey(new Object[]{100L, 1000L, true}));
        splitPointsReversedEdge.add(new PartitionKey(new Object[]{300L, 2000L, false}));
        graphPartitioner.addGroupPartitionerForReversedEdges(TestGroups.EDGE, new GroupPartitioner(TestGroups.EDGE, splitPointsReversedEdge));
        elements.add(createEdgeForEdgeGroup(5L, 5000L, true));
        elements.add(createEdgeForEdgeGroup(5L, 200L, false));
        elements.add(createEdgeForEdgeGroup(1000L, 90L, true));
        elements.add(createEdgeForEdgeGroup(1000L, 10000L, false));
        elements.add(createEdgeForEdgeGroup(1000L, 30000L, false));
        elements.add(createEdgeForEdgeGroup(1000L, 300000L, true));
        elements.add(createEdgeForEdgeGroup(10000L, 400L, false));
        // TestGroups.EDGE_2, split points are [10L, 2000L, true] and [100L, 1000L, false]. Create data with
        //        SOURCE   DESTINATION    DIRECTED
        //          5L         5000L        true
        //         10L         2000L        false
        //         10L         2000L        true
        //         10L         3000L        false
        //        100L         1000L        false
        //        100L         3000L        false
        //        100L         3000L        true
        final List<PartitionKey> splitPointsEdge_2 = new ArrayList<>();
        splitPointsEdge_2.add(new PartitionKey(new Object[]{10L, 2000L, true}));
        splitPointsEdge_2.add(new PartitionKey(new Object[]{100L, 1000L, false}));
        graphPartitioner.addGroupPartitioner(TestGroups.EDGE_2, new GroupPartitioner(TestGroups.EDGE_2, splitPointsEdge_2));
        final List<PartitionKey> splitPointsReversedEdge_2 = new ArrayList<>();
        splitPointsReversedEdge_2.add(new PartitionKey(new Object[]{1000L, 1500L, true}));
        splitPointsReversedEdge_2.add(new PartitionKey(new Object[]{2000L, 2500L, false}));
        graphPartitioner.addGroupPartitionerForReversedEdges(TestGroups.EDGE_2, new GroupPartitioner(TestGroups.EDGE_2, splitPointsReversedEdge_2));
        elements.add(createEdgeForEdgeGroup_2(5L, 5000L, true));
        elements.add(createEdgeForEdgeGroup_2(10L, 2000L, false));
        elements.add(createEdgeForEdgeGroup_2(10L, 2000L, true));
        elements.add(createEdgeForEdgeGroup_2(10L, 3000L, false));
        elements.add(createEdgeForEdgeGroup_2(100L, 1000L, false));
        elements.add(createEdgeForEdgeGroup_2(100L, 3000L, false));
        elements.add(createEdgeForEdgeGroup_2(100L, 3000L, true));
        final BiFunction<String, Integer, String> fileNameForGroupAndPartitionId = (group, partitionId) ->
                tempFilesDir + "/GROUP=" + group + "/split-" + partitionId;
        final BiFunction<String, Integer, String> fileNameForGroupAndPartitionIdForReversedEdge = (group, partitionId) ->
                tempFilesDir + "/REVERSED-GROUP=" + group + "/split-" + partitionId;
        final WriteUnsortedData writeUnsortedData = new WriteUnsortedData(tempFilesDir, CompressionCodecName.GZIP,
                schemaUtils, graphPartitioner, fileNameForGroupAndPartitionId,
                fileNameForGroupAndPartitionIdForReversedEdge);

        // When
        writeUnsortedData.writeElements(elements);

        // Then
        //  - For each group, directories split0, split1 and split2 should exist and each contain one file
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY + "/split-2", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY_2 + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY_2 + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.ENTITY_2 + "/split-2", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE + "/split-2", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE_2 + "/split-0", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE_2 + "/split-1", 1);
        testExistsAndContainsNFiles(tempFilesDir + "/GROUP=" + TestGroups.EDGE_2 + "/split-2", 1);
        //  - Each split file should contain the data for that split in the order it was written
        for (final String group : new HashSet<>(Arrays.asList(TestGroups.ENTITY, TestGroups.ENTITY_2))) {
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-0",
                    group, true, false, null, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-1",
                    group, true, false, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(1), elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-2",
                    group, true, false, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(1),
                    null, elements, schemaUtils);
        }
        for (final String group : new HashSet<>(Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2))) {
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-0",
                    group, false, false, null, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/REVERSED-GROUP=" + group + "/split-0",
                    group, false, true, null, graphPartitioner.getGroupPartitionerForReversedEdges(group).getIthPartitionKey(0),
                    elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-1",
                    group, false, false, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(0),
                    graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(1), elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/REVERSED-GROUP=" + group + "/split-1",
                    group, false, true, graphPartitioner.getGroupPartitionerForReversedEdges(group).getIthPartitionKey(0),
                    graphPartitioner.getGroupPartitionerForReversedEdges(group).getIthPartitionKey(1), elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/GROUP=" + group + "/split-2",
                    group, false, false, graphPartitioner.getGroupPartitioner(group).getIthPartitionKey(1),
                    null, elements, schemaUtils);
            testSplitFileContainsCorrectData(tempFilesDir + "/REVERSED-GROUP=" + group + "/split-2",
                    group, false, true, graphPartitioner.getGroupPartitionerForReversedEdges(group).getIthPartitionKey(1),
                    null, elements, schemaUtils);
        }
    }

    private Entity createEntityForEntityGroup(final long vertex) {
        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        entity.putProperty("date", date0);
        addPropertiesOtherThanVertexAndDate(entity);
        return entity;
    }

    public static Entity createEntityForEntityGroup_2(final long vertex) {
        final Entity entity = new Entity(TestGroups.ENTITY_2, vertex);
        entity.putProperty("date", new Date(1000000L));
        addPropertiesOtherThanVertexAndDate(entity);
        return entity;
    }

    public static Edge createEdgeForEdgeGroup(final long source, final long destination, final boolean directed) {
        final Edge edge = new Edge(TestGroups.EDGE, source, destination, directed);
        edge.putProperty("date", date0);
        addPropertiesOtherThanVertexAndDate(edge);
        return edge;
    }

    public static Edge createEdgeForEdgeGroup(final long source, final long destination, final boolean directed, final Date date) {
        final Edge edge = new Edge(TestGroups.EDGE, source, destination, directed);
        edge.putProperty("date", date);
        addPropertiesOtherThanVertexAndDate(edge);
        return edge;
    }

    public static Edge createEdgeForEdgeGroup(final long source,
                                              final long destination,
                                              final boolean directed,
                                              final Date date,
                                              final short multiplier) {
        final Edge edge = createEdgeForEdgeGroup(source, destination, directed, date);
        edge.putProperty("float", ((float) edge.getProperty("float")) * multiplier);
        edge.putProperty("long", ((long) edge.getProperty("long")) * multiplier);
        edge.putProperty("short", (short) ((short) edge.getProperty("short")) * multiplier);
        edge.putProperty("count", ((int) edge.getProperty("count")) * multiplier);
        final FreqMap freqMap = (FreqMap) edge.getProperty("freqMap");
        for (final Map.Entry<String, Long> entry : freqMap.entrySet()) {
            freqMap.put(entry.getKey(), entry.getValue() * multiplier);
        }
        edge.putProperty("freqMap", freqMap);
        return edge;
    }

    public static Edge createEdgeForEdgeGroup_2(final long source, final long destination, final boolean directed) {
        final Edge edge = new Edge(TestGroups.EDGE_2, source, destination, directed);
        edge.putProperty("date", new Date(1000L));
        addPropertiesOtherThanVertexAndDate(edge);
        return edge;
    }

    private static void addPropertiesOtherThanVertexAndDate(final Element element) {
        element.putProperty("byte", (byte) 50);
        element.putProperty("float", 50F);
        element.putProperty("treeSet", TestUtils.getTreeSet1());
        element.putProperty("long", 50L);
        element.putProperty("short", (short) 50);
        element.putProperty("freqMap", TestUtils.getFreqMap1());
        element.putProperty("count", 100);
    }

    private static void testExistsAndContainsNFiles(final String dir, final int numFiles) throws IOException {
        final Path path = new Path(dir);
        assertTrue(fs.exists(path));
        final FileStatus[] files = fs.listStatus(path, path1 -> path1.getName().endsWith(".parquet"));
        assertEquals(numFiles, files.length);
    }

    private static void testContainsCorrectDataNoSplitPoints(final String group,
                                                             final String file,
                                                             final List<Element> originalElements,
                                                             final SchemaUtils schemaUtils) throws IOException {
        final List<Element> elementsInThisGroup = originalElements.stream()
                .filter(e -> e.getGroup().equals(group))
                .collect(Collectors.toList());
        testContainsCorrectData(file, elementsInThisGroup,
                schemaUtils.getGafferSchema().getEntityGroups().contains(group),
                schemaUtils.getConverter(group));
    }

    private static void testSplitFileContainsCorrectData(final String file,
                                                         final String group,
                                                         final boolean isEntity,
                                                         final boolean reversed,
                                                         final PartitionKey minPartitionKey,
                                                         final PartitionKey maxPartitionKey,
                                                         final List<Element> elements,
                                                         final SchemaUtils schemaUtils) throws IOException {
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        final List<Element> elementsForSplit = elements.stream()
                .filter(e -> e.getGroup().equals(group))
                .filter(e -> {
                    try {
                        final PartitionKey convertedProps = reversed ?
                                new PartitionKey(converter.corePropertiesToParquetObjectsForReversedEdge((Edge) e))
                                : new PartitionKey(converter.corePropertiesToParquetObjects(e));
                        if (null == minPartitionKey) {
                            return convertedProps.compareTo(maxPartitionKey) < 0;
                        } else if (null == maxPartitionKey) {
                            return minPartitionKey.compareTo(convertedProps) <= 0;
                        } else {
                            return minPartitionKey.compareTo(convertedProps) <= 0 && convertedProps.compareTo(maxPartitionKey) < 0;
                        }
                    } catch (final SerialisationException exception) {
                        fail("Exception: " + exception.getMessage());
                    }
                    return true;
                })
                .collect(Collectors.toList());
        testContainsCorrectData(file, elementsForSplit, isEntity, converter);
    }

    private static void testContainsCorrectData(final String file,
                                                final List<Element> originalElements,
                                                final boolean isEntity,
                                                final GafferGroupObjectConverter converter) throws IOException {
        // Read Parquet file and convert back to Elements
        final ParquetReader<Element> reader = new ParquetElementReader.Builder<Element>(new Path(file))
                .isEntity(isEntity).usingConverter(converter).build();
        final List<Element> readElements = new ArrayList<>();
        Element element = reader.read();
        while (null != element) {
            readElements.add(element);
            element = reader.read();
        }
        reader.close();
        // Check same number of elements
        assertEquals(originalElements.size(), readElements.size());
        // Check elements are the same and in the same order
        for (int i = 0; i < originalElements.size(); i++) {
            assertEquals(originalElements.get(i), readElements.get(i));
        }
    }

    private List<Element> getData(final long num) {
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEntity(TestGroups.ENTITY, i, (byte) 1, 2.0F, TestUtils.getTreeSet1(),
                    5L, (short) 6, new Date(), TestUtils.getFreqMap1(), 1, null));
            data.add(DataGen.getEntity(TestGroups.ENTITY_2, i + 5, (byte) 2, 4.0F,
                    TestUtils.getTreeSet2(), 6L, (short) 7, new Date(), TestUtils.getFreqMap2(), 1, null));
            data.add(DataGen.getEdge(TestGroups.EDGE, i, i + 1, true, (byte) 1, 2.0F,
                    TestUtils.getTreeSet1(), 5L, (short) 6, new Date(), TestUtils.getFreqMap1(), 1, null));
            data.add(DataGen.getEdge(TestGroups.EDGE_2, num - i, 1L, true, (byte) 1, 2.0F,
                    TestUtils.getTreeSet1(), 5L, (short) 6, new Date(), TestUtils.getFreqMap1(), 1, null));
        }
        return data;
    }
}
