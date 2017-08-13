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
 * limitations under the License
 */

package uk.gov.gchq.gaffer.parquetstore.operation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TypeValueVertexOperationsTest extends AbstractOperationsTest {
    
    @BeforeClass
    public static void genData() throws OperationException {
        Logger.getRootLogger().setLevel(Level.WARN);
        getGraph().execute(new AddElements.Builder().input(getElements()).build(), USER);
    }

    @Before
    public void setup() {
        graph = getGraph();
    }

    private static Graph getGraph() {
        final ParquetStoreProperties pp = (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                AbstractOperationsTest.class.getResourceAsStream("/multiUseStore.properties"));
        return new Graph.Builder()
                .addSchema(getSchema())
                .storeProperties(pp)
                .graphId("test")
                .build();
    }
    
    protected static Schema getSchema() {
        return Schema.fromJson(StreamUtil.openStreams(TypeValueVertexOperationsTest.class, "schemaUsingTypeValueVertexType"));
    }

    private static Iterable<? extends Element> getElements() {
        return DataGen.generate300TypeValueElements();
    }

    @Override
    public void setupSeeds() {
        seedsList = new ArrayList<>(4);
        seedsList.add(new EntitySeed(new TypeValue("type0", "vrt10")));
        seedsList.add(new EntitySeed(new TypeValue("type2", "src17")));
        seedsList.add(new EdgeSeed(new TypeValue("type1", "src11"), new TypeValue("type1", "dst12"), true));
        seedsList.add(new EdgeSeed(new TypeValue("type1", "src1"), new TypeValue("type1", "dst2"), false));
    }

    @Override
    public void setupView() {
        view = new View.Builder()
            .edge(TestGroups.EDGE,
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select("treeSet", "double")
                            .execute(
                                new Or.Builder()
                                    .select(0)
                                    .execute(new Not<>(new IsEqual(TestUtils.MERGED_TREESET)))
                                    .select(1)
                                    .execute(new IsMoreThan(3.0, true))
                                    .build())
                            .build())
                    .build())
            .entity(TestGroups.ENTITY,
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select(ParquetStoreConstants.VERTEX + "_type")
                            .execute(new IsEqual("type0"))
                            .build())
                    .transientProperty(ParquetStoreConstants.VERTEX + "_type", String.class)
                    .build())
            .build();
    }

    @Override
    protected void checkData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(175);
        final List<Element> actual = new ArrayList<>(175);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        for (int x = 0 ; x < 25; x++) {
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'a', 0.2 * x, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));

            expected.add(DataGen.getEdge(TestGroups.EDGE_2, src, dst, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEdge(TestGroups.EDGE_2, src, dst, false, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));

            expected.add(DataGen.getEntity(TestGroups.ENTITY, vrt, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEntity(TestGroups.ENTITY_2, vrt, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetSeededElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(9);
        final List<Element> actual = new ArrayList<>(9);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type1", "src1"), new TypeValue("type1", "dst2"), false, (byte) 'a', 0.2, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type1", "src1"), new TypeValue("type1", "dst2"), false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 6L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type1", "src11"), new TypeValue("type1", "dst12"), true, (byte) 'b', 2.5, 6f, TestUtils.MERGED_TREESET, 71L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type2", "src17"), new TypeValue("type2", "dst18"), true, (byte) 'b', 3.7, 6f, TestUtils.MERGED_TREESET, 107L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, new TypeValue("type1", "src1"), new TypeValue("type1", "dst2"), false, (byte) 'b', 0.5, 6f, TestUtils.MERGED_TREESET, 11L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, new TypeValue("type1", "src11"), new TypeValue("type1", "dst12"), true, (byte) 'b', 2.5, 6f, TestUtils.MERGED_TREESET, 71L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, new TypeValue("type2", "src17"), new TypeValue("type2", "dst18"), true, (byte) 'b', 3.7, 6f, TestUtils.MERGED_TREESET, 107L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, new TypeValue("type0", "vrt10"), (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 110L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, new TypeValue("type0", "vrt10"), (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 110L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(21);
        final List<Element> actual = new ArrayList<>(21);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type0", "src15"), new TypeValue("type0", "dst16"), false, (byte) 'a', 3.0, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type0", "src15"), new TypeValue("type0", "dst16"), true, (byte) 'b', 3.3, 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type0", "src20"), new TypeValue("type0", "dst21"), false, (byte) 'a', 4.0, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type0", "src20"), new TypeValue("type0", "dst21"), true, (byte) 'b', 4.3, 6f, TestUtils.MERGED_TREESET, 125L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type1", "src16"), new TypeValue("type1", "dst17"), false, (byte) 'a', 3.2, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type1", "src16"), new TypeValue("type1", "dst17"), true, (byte) 'b', 3.5, 6f, TestUtils.MERGED_TREESET, 101L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type1", "src21"), new TypeValue("type1", "dst22"), false, (byte) 'a', 4.2, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type1", "src21"), new TypeValue("type1", "dst22"), true, (byte) 'b', 4.5, 6f, TestUtils.MERGED_TREESET, 131L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type2", "src17"), new TypeValue("type2", "dst18"), false, (byte) 'a', 3.4000000000000004, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type2", "src17"), new TypeValue("type2", "dst18"), true, (byte) 'b', 3.7, 6f, TestUtils.MERGED_TREESET, 107L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type2", "src22"), new TypeValue("type2", "dst23"), false, (byte) 'a', 4.4, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type2", "src22"), new TypeValue("type2", "dst23"), true, (byte) 'b', 4.7, 6f, TestUtils.MERGED_TREESET, 137L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type3", "src18"), new TypeValue("type3", "dst19"), false, (byte) 'a', 3.6, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type3", "src18"), new TypeValue("type3", "dst19"), true, (byte) 'b', 3.9, 6f, TestUtils.MERGED_TREESET, 113L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type3", "src23"), new TypeValue("type3", "dst24"), false, (byte) 'a', 4.6000000000000005, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type3", "src23"), new TypeValue("type3", "dst24"), true, (byte) 'b', 4.9, 6f, TestUtils.MERGED_TREESET, 143L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type4", "src14"), new TypeValue("type4", "dst15"), true, (byte) 'b', 3.1, 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type4", "src19"), new TypeValue("type4", "dst20"), false, (byte) 'a', 3.8000000000000003, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type4", "src19"), new TypeValue("type4", "dst20"), true, (byte) 'b', 4.1000000000000005, 6f, TestUtils.MERGED_TREESET, 119L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type4", "src24"), new TypeValue("type4", "dst25"), false, (byte) 'a', 4.800000000000001, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type4", "src24"), new TypeValue("type4", "dst25"), true, (byte) 'b', 5.1000000000000005, 6f, TestUtils.MERGED_TREESET, 149L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetSeededAndFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(1);
        final List<Element> actual = new ArrayList<>(1);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge(TestGroups.EDGE, new TypeValue("type2", "src17"), new TypeValue("type2", "dst18"), true, (byte) 'b', 3.7, 6f, TestUtils.MERGED_TREESET, 107L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }
}
