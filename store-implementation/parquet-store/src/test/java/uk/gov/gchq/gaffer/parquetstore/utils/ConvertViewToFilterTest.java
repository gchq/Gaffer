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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.ColumnIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GroupIndex;
import uk.gov.gchq.gaffer.parquetstore.index.MinValuesWithPath;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ConvertViewToFilterTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private ParquetFilterUtils filterUtils;
    private GraphIndex graphIndex;
    private Set<Path> expectedPaths;

    @Before
    public void setUp() throws IOException, StoreException {
        final Schema schema = TestUtils.gafferSchema("schemaUsingTypeValueVertexType");
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final ParquetStore store = new ParquetStore();
        store.initialise("ConvertViewToFilterTest", schema, properties);
        filterUtils = new ParquetFilterUtils(store);
        graphIndex = new GraphIndex();
        final GroupIndex groupIndex = new GroupIndex();
        graphIndex.add(TestGroups.ENTITY, groupIndex);
        final ColumnIndex vertIndex = new ColumnIndex();
        groupIndex.add(ParquetStoreConstants.VERTEX, vertIndex);
        final MinValuesWithPath file1 = new MinValuesWithPath(new Object[]{"test", "val"}, "file1");
        vertIndex.add(file1);
        final MinValuesWithPath file2 = new MinValuesWithPath(new Object[]{"type", "value"}, "file2");
        vertIndex.add(file2);
        expectedPaths = new HashSet<>();
        expectedPaths.add(new Path(properties.getDataDir() + "/ConvertViewToFilterTest/0/graph/GROUP=BasicEntity/file1"));
        expectedPaths.add(new Path(properties.getDataDir() + "/ConvertViewToFilterTest/0/graph/GROUP=BasicEntity/file2"));
    }

    @After
    public void cleanUp() {
        filterUtils = null;
    }

    @Test
    public void getBasicGroupFilterTest() throws OperationException, SerialisationException {
        final View view = new View.Builder().entity(TestGroups.ENTITY,
                new ViewElementDefinition.Builder().preAggregationFilter(
                        new ElementFilter.Builder()
                                .select("double")
                                .execute(
                                        new IsEqual(2.0))
                                .build())
                        .build())
                .build();
        filterUtils.buildPathToFilterMap(view, DirectedType.EITHER, SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, SeedMatching.SeedMatchingType.EQUAL, new ArrayList<>(), graphIndex);
        final Pair<FilterPredicate, Set<Path>> groupFilterWithPaths = filterUtils.buildGroupFilter(TestGroups.ENTITY, true);
        final FilterPredicate filter = groupFilterWithPaths.getFirst();
        final FilterPredicate expected = eq(doubleColumn("double"), 2.0);
        assertEquals(expected, filter);
        assertThat(expectedPaths, containsInAnyOrder(groupFilterWithPaths.getSecond().toArray()));
    }

    @Test
    public void getMultiColumnGroupFilterTest() throws OperationException, IOException {
        final View view = new View.Builder().entity(TestGroups.ENTITY,
                new ViewElementDefinition.Builder().preAggregationFilter(
                        new ElementFilter.Builder()
                                .select(ParquetStoreConstants.VERTEX)
                                .execute(
                                        new IsEqual(new TypeValue("type", "value")))
                                .build())
                        .build())
                .build();
        filterUtils.buildPathToFilterMap(view, DirectedType.EITHER, SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, SeedMatching.SeedMatchingType.EQUAL, new ArrayList<>(), graphIndex);
        final Pair<FilterPredicate, Set<Path>> groupFilterWithPaths = filterUtils.buildGroupFilter(TestGroups.ENTITY, true);
        final FilterPredicate filter = groupFilterWithPaths.getFirst();
        final FilterPredicate expected = and(eq(binaryColumn("VERTEX_type"), Binary.fromString("type")), eq(binaryColumn("VERTEX_value"), Binary.fromString("value")));
        assertEquals(expected, filter);
        assertThat(expectedPaths, containsInAnyOrder(groupFilterWithPaths.getSecond().toArray()));
    }

    @Test
    public void getNestedGroupFilterTest() throws OperationException, IOException {
        final View view = new View.Builder().entity(TestGroups.ENTITY,
                new ViewElementDefinition.Builder().preAggregationFilter(
                        new ElementFilter.Builder()
                                .select("freqMap.type_value.key")
                                .execute(
                                        new IsEqual("test"))
                                .build())
                        .build())
                .build();
        filterUtils.buildPathToFilterMap(view, DirectedType.EITHER, SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, SeedMatching.SeedMatchingType.EQUAL, new ArrayList<>(), graphIndex);
        final Pair<FilterPredicate, Set<Path>> groupFilterWithPaths = filterUtils.buildGroupFilter(TestGroups.ENTITY, true);
        final FilterPredicate filter = groupFilterWithPaths.getFirst();
        final FilterPredicate expected = eq(binaryColumn("freqMap.type_value.key"), Binary.fromString("test"));
        assertEquals(expected, filter);
        assertThat(expectedPaths, containsInAnyOrder(groupFilterWithPaths.getSecond().toArray()));
    }
}
