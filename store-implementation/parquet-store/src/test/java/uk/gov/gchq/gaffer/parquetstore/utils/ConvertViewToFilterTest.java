package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
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
    private ParquetFilterUtils filterUtils;
    private GraphIndex graphIndex;
    private Set<Path> expectedPaths;

    @Before
    public void setUp() throws StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(ConvertViewToFilterTest.class, "schemaUsingTypeValueVertexType"));
        final ParquetStore store = new ParquetStore();
        store.initialise("ConvertViewToFilterTest", schema, new ParquetStoreProperties());
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
        expectedPaths.add(new Path("parquet_data/ConvertViewToFilterTest/0/graph/GROUP=BasicEntity/file1"));
        expectedPaths.add(new Path("parquet_data/ConvertViewToFilterTest/0/graph/GROUP=BasicEntity/file2"));
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
