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
package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
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
import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * This class tests the functionality of the ParquetFilterUtils methods
 */
public class ParquetFilterUtilsTest {

    @Test
    public void buildPathToFilterMap() throws StoreException, SerialisationException, OperationException {
        final ParquetStore store = new ParquetStore();
        final Schema gafferSchema = TestUtils.gafferSchema("schemaUsingStringVertexType");
        final ParquetStoreProperties storeProperties = TestUtils.getParquetStoreProperties();
        store.initialise("buildPathToFilterMap", gafferSchema, storeProperties);
        final ParquetFilterUtils parquetFilterUtils = new ParquetFilterUtils(store);
        
        final ElementFilter filter = new ElementFilter.Builder()
                .select(ParquetStoreConstants.SOURCE)
                .execute(
                        new And.Builder()
                                .select(0)
                                .execute(
                                        new IsMoreThan("c1"))
                                .select(0)
                                .execute(
                                        new IsLessThan("e"))
                                .build())
                .build();
        final View view = new View.Builder().edge(TestGroups.EDGE, 
                new ViewElementDefinition.Builder()
                        .preAggregationFilter(filter)
                        .build())
                .build();
        final DirectedType directedType = DirectedType.EITHER;
        final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType = SeededGraphFilters.IncludeIncomingOutgoingType.EITHER;
        final SeedMatching.SeedMatchingType seedMatchingType = SeedMatching.SeedMatchingType.RELATED;
        final Iterable<? extends ElementId> seeds = null;
        final GraphIndex graphIndex = new GraphIndex();
        final GroupIndex edgeIndex = new GroupIndex();
        graphIndex.add(TestGroups.EDGE, edgeIndex);
        final ColumnIndex srcIndex = new ColumnIndex();
        edgeIndex.add(ParquetStoreConstants.SOURCE, srcIndex);
        srcIndex.add(new MinValuesWithPath(new Object[]{"a"}, "srcFileA"));
        srcIndex.add(new MinValuesWithPath(new Object[]{"b"}, "srcFileB"));
        srcIndex.add(new MinValuesWithPath(new Object[]{"c"}, "srcFileC"));
        srcIndex.add(new MinValuesWithPath(new Object[]{"d"}, "srcFileD"));
        srcIndex.add(new MinValuesWithPath(new Object[]{"e"}, "srcFileE"));
        srcIndex.add(new MinValuesWithPath(new Object[]{"f"}, "srcFileF"));
        srcIndex.add(new MinValuesWithPath(new Object[]{"g"}, "srcFileG"));
        srcIndex.add(new MinValuesWithPath(new Object[]{"h"}, "srcFileH"));
        final ColumnIndex dstIndex = new ColumnIndex();
        edgeIndex.add(ParquetStoreConstants.DESTINATION, dstIndex);
        dstIndex.add(new MinValuesWithPath(new Object[]{"a"}, "dstFileA"));
        dstIndex.add(new MinValuesWithPath(new Object[]{"b"}, "dstFileB"));
        dstIndex.add(new MinValuesWithPath(new Object[]{"c"}, "dstFileC"));
        dstIndex.add(new MinValuesWithPath(new Object[]{"d"}, "dstFileD"));
        dstIndex.add(new MinValuesWithPath(new Object[]{"e"}, "dstFileE"));
        dstIndex.add(new MinValuesWithPath(new Object[]{"f"}, "dstFileF"));
        dstIndex.add(new MinValuesWithPath(new Object[]{"g"}, "dstFileG"));
        dstIndex.add(new MinValuesWithPath(new Object[]{"h"}, "dstFileH"));

        parquetFilterUtils.buildPathToFilterMap(view, directedType, includeIncomingOutgoingType, seedMatchingType, seeds, graphIndex);
        final Map<Path, FilterPredicate> pathToFilterMap = parquetFilterUtils.getPathToFilterMap();
        final Set<String> expectedPaths = new HashSet<>(3);
        expectedPaths.add("srcFileC");
        expectedPaths.add("srcFileD");
        expectedPaths.add("srcFileE");
        final Object[] actualPaths = pathToFilterMap.keySet().stream().map(Path::getName).toArray();
        assertThat(expectedPaths, containsInAnyOrder(actualPaths));
    }
            
}
