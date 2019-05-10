/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.query;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.LongVertexOperationsTest;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.CalculatePartitionerTest;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class QueryGeneratorTest {

    private Schema schema = new LongVertexOperationsTest().createSchema();

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void testQueryGeneratorForGetAllElements() throws IOException, OperationException {
        // Given
        // - Create snapshot folder
        final String folder = "file:///" + testFolder.newFolder().toString();
        final String snapshotFolder = folder + "/" + ParquetStore.getSnapshotPath(1000L);
        // - Write out Parquet files so know the partitioning
        CalculatePartitionerTest.writeData(snapshotFolder, new SchemaUtils(schema));
        // - Initialise store
        final ParquetStoreProperties storeProperties = new ParquetStoreProperties();
        storeProperties.setDataDir(folder);
        storeProperties.setTempFilesDir(folder + "/tmpdata");
        final ParquetStore store = (ParquetStore) ParquetStore.createStore("graphId", schema, storeProperties);

        // When 1 - no view
        GetAllElements getAllElements = new GetAllElements.Builder().build();
        ParquetQuery query = new QueryGenerator(store).getParquetQuery(getAllElements);

        // Then 1
        final List<ParquetFileQuery> expected = new ArrayList<>();
        for (final String group : Arrays.asList(TestGroups.ENTITY, TestGroups.ENTITY_2, TestGroups.EDGE, TestGroups.EDGE_2)) {
            final Path groupFolderPath = store.getGroupPath(group);
            for (int partition = 0; partition < 10; partition++) {
                final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
                expected.add(new ParquetFileQuery(pathForPartitionFile, null, true));
            }
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 2 - simple view that restricts to one group
        getAllElements = new GetAllElements.Builder().view(new View.Builder().edge(TestGroups.EDGE).build()).build();
        query = new QueryGenerator(store).getParquetQuery(getAllElements);

        // Then 2
        expected.clear();
        Path groupFolderPath = store.getGroupPath(TestGroups.EDGE);
        for (int partition = 0; partition < 10; partition++) {
            final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
            expected.add(new ParquetFileQuery(pathForPartitionFile, null, true));
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 3 - view with filter that can be pushed down to Parquet
        getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(10))
                                        .build())
                                .build())
                        .build())
                .build();
        query = new QueryGenerator(store).getParquetQuery(getAllElements);

        // Then 3
        expected.clear();
        for (int partition = 0; partition < 10; partition++) {
            final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
            expected.add(new ParquetFileQuery(pathForPartitionFile, gt(FilterApi.intColumn("count"), 10), true));
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 4 - view with filter that can't be pushed down to Parquet
        getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsEvenFilter())
                                        .build())
                                .build())
                        .build())
                .build();
        query = new QueryGenerator(store).getParquetQuery(getAllElements);

        // Then 4
        expected.clear();
        for (int partition = 0; partition < 10; partition++) {
            final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
            expected.add(new ParquetFileQuery(pathForPartitionFile, null, false));
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 5 - view with one filter that can be pushed down and one that can't
        getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsEvenFilter())
                                        .select("count")
                                        .execute(new IsMoreThan(10))
                                        .build())
                                .build())
                        .build())
                .build();
        query = new QueryGenerator(store).getParquetQuery(getAllElements);

        // Then 5
        expected.clear();
        for (int partition = 0; partition < 10; partition++) {
            final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(partition));
            expected.add(new ParquetFileQuery(pathForPartitionFile, gt(FilterApi.intColumn("count"), 10), false));
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));
    }

    @Test
    public void testQueryGeneratorForGetElementsWithEntitySeeds() throws IOException, OperationException {
        // Given
        // - Create snapshot folder
        final String folder = "file:///" + testFolder.newFolder().toString();
        final String snapshotFolder = folder + "/" + ParquetStore.getSnapshotPath(1000L);
        // - Write out Parquet files so know the partitioning
        CalculatePartitionerTest.writeData(snapshotFolder, new SchemaUtils(schema));
        // - Initialise store
        final ParquetStoreProperties storeProperties = new ParquetStoreProperties();
        storeProperties.setDataDir(folder);
        storeProperties.setTempFilesDir(folder + "/tmpdata");
        final ParquetStore store = (ParquetStore) ParquetStore.createStore("graphId", schema, storeProperties);

        // When 1 - no view, query for vertex 0
        GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(0L))
                .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                .build();
        ParquetQuery query = new QueryGenerator(store).getParquetQuery(getElements);

        // Then 1
        final List<ParquetFileQuery> expected = new ArrayList<>();
        final FilterPredicate vertex0 = eq(FilterApi.longColumn(ParquetStore.VERTEX), 0L);
        final FilterPredicate source0 = eq(FilterApi.longColumn(ParquetStore.SOURCE), 0L);
        final FilterPredicate destination0 = eq(FilterApi.longColumn(ParquetStore.DESTINATION), 0L);
        for (final String group : Arrays.asList(TestGroups.ENTITY, TestGroups.ENTITY_2)) {
            final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, false));
            final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartitionFile, vertex0, true));
        }
        for (final String group : Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2)) {
            final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, false));
            final Path pathForPartitionFile = new Path(groupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartitionFile, source0, true));
            final Path reversedGroupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, true));
            final Path pathForReversedPartitionFile = new Path(reversedGroupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForReversedPartitionFile, destination0, true));
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 2 - no view, query for vertices 0 and 1000000
        getElements = new GetElements.Builder()
                .input(new EntitySeed(0L), new EntitySeed(1000000L))
                .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                .build();
        query = new QueryGenerator(store).getParquetQuery(getElements);

        // Then 2
        expected.clear();
        final FilterPredicate vertex1000000 = eq(FilterApi.longColumn(ParquetStore.VERTEX), 1000000L);
        final FilterPredicate source1000000 = eq(FilterApi.longColumn(ParquetStore.SOURCE), 1000000L);
        final FilterPredicate destination1000000 = eq(FilterApi.longColumn(ParquetStore.DESTINATION), 1000000L);
        for (final String group : Arrays.asList(TestGroups.ENTITY, TestGroups.ENTITY_2)) {
            final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, false));
            final Path pathForPartitionFile1 = new Path(groupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartitionFile1, vertex0, true));
            final Path pathForPartitionFile2 = new Path(groupFolderPath, ParquetStore.getFile(9));
            expected.add(new ParquetFileQuery(pathForPartitionFile2, vertex1000000, true));
        }
        for (final String group : Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2)) {
            final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, false));
            final Path reversedGroupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, true));
            // Partition 0, vertex 0L
            final Path pathForPartitionFile1 = new Path(groupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartitionFile1, source0, true));
            // Partition 9, vertex 1000000L
            final Path pathForPartitionFile2 = new Path(groupFolderPath, ParquetStore.getFile(9));
            expected.add(new ParquetFileQuery(pathForPartitionFile2, source1000000, true));
            // Partition 0 of reversed, vertex 0L
            final Path pathForPartitionFile3 = new Path(reversedGroupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartitionFile3, destination0, true));
            // Partition 9 of reversed, vertex 1000000L
            final Path pathForPartitionFile4 = new Path(reversedGroupFolderPath, ParquetStore.getFile(9));
            expected.add(new ParquetFileQuery(pathForPartitionFile4, destination1000000, true));
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 3 - view with filter that can be pushed down to Parquet, query for vertices 0 and 1000000
        getElements = new GetElements.Builder()
                .input(new EntitySeed(0L), new EntitySeed(1000000L))
                .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(10))
                                        .build())
                                .build())
                        .build())
                .build();
        query = new QueryGenerator(store).getParquetQuery(getElements);

        // Then 3
        expected.clear();
        final FilterPredicate source0AndCount = and(gt(FilterApi.intColumn("count"), 10),
                eq(FilterApi.longColumn(ParquetStore.SOURCE), 0L));
        final FilterPredicate source1000000AndCount = and(gt(FilterApi.intColumn("count"), 10),
                eq(FilterApi.longColumn(ParquetStore.SOURCE), 1000000L));
        final FilterPredicate destination0AndCount = and(gt(FilterApi.intColumn("count"), 10),
                eq(FilterApi.longColumn(ParquetStore.DESTINATION), 0L));
        final FilterPredicate destination1000000AndCount = and(gt(FilterApi.intColumn("count"), 10),
                eq(FilterApi.longColumn(ParquetStore.DESTINATION), 1000000L));
        final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(TestGroups.EDGE, false));
        final Path reversedGroupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(TestGroups.EDGE, true));
        // Partition 0, vertex 0L
        final Path pathForPartitionFile1 = new Path(groupFolderPath, ParquetStore.getFile(0));
        expected.add(new ParquetFileQuery(pathForPartitionFile1, source0AndCount, true));
        // Partition 9, vertex 1000000L
        final Path pathForPartitionFile2 = new Path(groupFolderPath, ParquetStore.getFile(9));
        expected.add(new ParquetFileQuery(pathForPartitionFile2, source1000000AndCount, true));
        // Partition 0 of reversed, vertex 0L
        final Path pathForPartitionFile3 = new Path(reversedGroupFolderPath, ParquetStore.getFile(0));
        expected.add(new ParquetFileQuery(pathForPartitionFile3, destination0AndCount, true));
        // Partition 9 of reversed, vertex 1000000L
        final Path pathForPartitionFile4 = new Path(reversedGroupFolderPath, ParquetStore.getFile(9));
        expected.add(new ParquetFileQuery(pathForPartitionFile4, destination1000000AndCount, true));
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 4 - view with filter that can't be pushed down to Parquet, query for vertices 0 and 1000000
        getElements = new GetElements.Builder()
                .input(new EntitySeed(0L), new EntitySeed(1000000L))
                .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsEvenFilter())
                                        .build())
                                .build())
                        .build())
                .build();
        query = new QueryGenerator(store).getParquetQuery(getElements);

        // Then 4
        expected.clear();
        // Partition 0, vertex 0L
        expected.add(new ParquetFileQuery(pathForPartitionFile1, source0, false));
        // Partition 9, vertex 1000000L
        expected.add(new ParquetFileQuery(pathForPartitionFile2, source1000000, false));
        // Partition 0 of reversed, vertex 0L
        expected.add(new ParquetFileQuery(pathForPartitionFile3, destination0, false));
        // Partition 9 of reversed, vertex 1000000L
        expected.add(new ParquetFileQuery(pathForPartitionFile4, destination1000000, false));
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));
    }

    @Test
    public void testQueryGeneratorForGetElementsWithEdgeSeeds() throws IOException, OperationException {
        // Given
        // - Create snapshot folder
        final String folder = "file:///" + testFolder.newFolder().toString();
        final String snapshotFolder = folder + "/" + ParquetStore.getSnapshotPath(1000L);
        // - Write out Parquet files so know the partitioning
        CalculatePartitionerTest.writeData(snapshotFolder, new SchemaUtils(schema));
        // - Initialise store
        final ParquetStoreProperties storeProperties = new ParquetStoreProperties();
        storeProperties.setDataDir(folder);
        storeProperties.setTempFilesDir(folder + "/tmpdata");
        final ParquetStore store = (ParquetStore) ParquetStore.createStore("graphId", schema, storeProperties);

        // When 1 - no view, query for edges 0->1, 10--10000, 10000--10 with seed matching type set to EQUAL
        GetElements getElements = new GetElements.Builder()
                .input(new EdgeSeed(0L, 1L, DirectedType.DIRECTED),
                        new EdgeSeed(10L, 1000L, DirectedType.UNDIRECTED),
                        new EdgeSeed(10000L, 10L, DirectedType.EITHER))
                .seedMatching(SeedMatching.SeedMatchingType.EQUAL)
                .build();
        ParquetQuery query = new QueryGenerator(store).getParquetQuery(getElements);

        // Then 1
        final List<ParquetFileQuery> expected = new ArrayList<>();
        final FilterPredicate source0 = eq(FilterApi.longColumn(ParquetStore.SOURCE), 0L);
        final FilterPredicate source10 = eq(FilterApi.longColumn(ParquetStore.SOURCE), 10L);
        final FilterPredicate source10000 = eq(FilterApi.longColumn(ParquetStore.SOURCE), 10000L);
        final FilterPredicate destination1 = eq(FilterApi.longColumn(ParquetStore.DESTINATION), 1L);
        final FilterPredicate destination10 = eq(FilterApi.longColumn(ParquetStore.DESTINATION), 10L);
        final FilterPredicate destination1000 = eq(FilterApi.longColumn(ParquetStore.DESTINATION), 1000L);
        final FilterPredicate directedTrue = eq(FilterApi.booleanColumn(ParquetStore.DIRECTED), true);
        final FilterPredicate directedFalse = eq(FilterApi.booleanColumn(ParquetStore.DIRECTED), false);
        final FilterPredicate source0Destination1DirectedTrue = and(and(source0, destination1), directedTrue);
        final FilterPredicate source10Destination1000DirectedFalse = and(and(source10, destination1000), directedFalse);
        final FilterPredicate source10000Destination10DirectedEither = and(source10000, destination10);
        for (final String group : Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2)) {
            final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, false));
            // 0->1 partition 0 of forward
            final Path pathForPartition0File = new Path(groupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartition0File, source0Destination1DirectedTrue, true)); // Comment here that don't need to look in the reversed directory
            // 10--1000 partition 1 of forward
            final Path pathForPartition1File = new Path(groupFolderPath, ParquetStore.getFile(1));
            expected.add(new ParquetFileQuery(pathForPartition1File, source10Destination1000DirectedFalse, true)); // Comment here that don't need to look in the reversed directory
            // 10000--10 partition 9 of forward
            final Path pathForPartition9File = new Path(groupFolderPath, ParquetStore.getFile(9));
            expected.add(new ParquetFileQuery(pathForPartition9File, source10000Destination10DirectedEither, true)); // Comment here that don't need to look in the reversed directory
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));

        // When 2 - no view, query for edges 0->1, 10--10000, 10000--10 with seed matching type set to RELATED
        getElements = new GetElements.Builder()
                .input(new EdgeSeed(0L, 1L, DirectedType.DIRECTED),
                        new EdgeSeed(10L, 1000L, DirectedType.UNDIRECTED),
                        new EdgeSeed(10000L, 10L, DirectedType.EITHER))
                .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                .build();
        query = new QueryGenerator(store).getParquetQuery(getElements);

        // Then 2
        expected.clear();
        final FilterPredicate vertex0 = eq(FilterApi.longColumn(ParquetStore.VERTEX), 0L);
        final FilterPredicate vertex1 = eq(FilterApi.longColumn(ParquetStore.VERTEX), 1L);
        final FilterPredicate vertex10 = eq(FilterApi.longColumn(ParquetStore.VERTEX), 10L);
        final FilterPredicate vertex1000 = eq(FilterApi.longColumn(ParquetStore.VERTEX), 1000L);
        final FilterPredicate vertex10000 = eq(FilterApi.longColumn(ParquetStore.VERTEX), 10000L);
        final FilterPredicate vertex0or1 = or(vertex0, vertex1);
        final FilterPredicate vertex10or1000 = or(vertex10, vertex1000);
        final FilterPredicate vertex10000or10 = or(vertex10000, vertex10);
        for (final String group : Arrays.asList(TestGroups.ENTITY, TestGroups.ENTITY_2)) {
            final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, false));
            // 0 and 1 in partition 0
            final Path pathForPartition0File = new Path(groupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartition0File, vertex0or1, true));
            // 10 or 1000 and 10000 or 10 in partition 1 (NB 1000 and 10000 't appear in partition 1 but this doesn't cause any incorrect results, and will be fixed in later versions)
            final Path pathForPartition1File = new Path(groupFolderPath, ParquetStore.getFile(1));
            expected.add(new ParquetFileQuery(pathForPartition1File, or(vertex10or1000, vertex10000or10), true));
            // 10 or 1000 and 1000 or 10000 in partition 9
            final Path pathForPartition9File = new Path(groupFolderPath, ParquetStore.getFile(9));
            expected.add(new ParquetFileQuery(pathForPartition9File, or(vertex10or1000, vertex10000or10), true));
        }
        for (final String group : Arrays.asList(TestGroups.EDGE, TestGroups.EDGE_2)) {
            final Path groupFolderPath = new Path(snapshotFolder, ParquetStore.getGroupSubDir(group, false));
            // 0->1 partition 0 of forward
            final Path pathForPartition0File = new Path(groupFolderPath, ParquetStore.getFile(0));
            expected.add(new ParquetFileQuery(pathForPartition0File, source0Destination1DirectedTrue, true)); // Comment here that don't need to look in the reversed directory
            // 10--1000 partition 1 of forward
            final Path pathForPartition1File = new Path(groupFolderPath, ParquetStore.getFile(1));
            expected.add(new ParquetFileQuery(pathForPartition1File, source10Destination1000DirectedFalse, true)); // Comment here that don't need to look in the reversed directory
            // 10000--10 partition 9 of forward
            final Path pathForPartition9File = new Path(groupFolderPath, ParquetStore.getFile(9));
            expected.add(new ParquetFileQuery(pathForPartition9File, source10000Destination10DirectedEither, true)); // Comment here that don't need to look in the reversed directory
        }
        assertThat(expected, containsInAnyOrder(query.getAllParquetFileQueries().toArray()));
    }

    public static class IsEvenFilter implements Predicate<Integer> {

        @Override
        public boolean test(final Integer integer) {
            return 0 == integer % 2;
        }
    }
}
