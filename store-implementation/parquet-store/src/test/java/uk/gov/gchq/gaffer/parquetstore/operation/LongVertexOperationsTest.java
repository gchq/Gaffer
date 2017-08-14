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
import uk.gov.gchq.gaffer.graph.GraphConfig;
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
import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LongVertexOperationsTest extends AbstractOperationsTest {

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
        ParquetStoreProperties pp = (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                AbstractOperationsTest.class.getResourceAsStream("/multiUseStore.properties"));
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("test")
                        .build())
                .addSchema(getSchema())
                .storeProperties(pp)
                .build();
    }

    protected static Schema getSchema() {
        return Schema.fromJson(StreamUtil.openStreams(LongVertexOperationsTest.class, "schemaUsingLongVertexType"));
    }

    private static Iterable<Element> getElements() {
        return DataGen.generate300LongElements();
    }

    @Override
    public void setupSeeds() {
        seedsList = new ArrayList<>(5);
        seedsList.add(new EntitySeed(5L));
        seedsList.add(new EntitySeed(15L));
        seedsList.add(new EntitySeed(10L));
        seedsList.add(new EdgeSeed(13L, 14L, true));
        seedsList.add(new EdgeSeed(2L, 3L, true));
    }

    @Override
    public void setupView() {
        view = new View.Builder()
                .edge(TestGroups.EDGE,
                        new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select("treeSet", "long")
                                                .execute(
                                                        new And.Builder()
                                                                .select(0)
                                                                .execute(new IsEqual(TestUtils.MERGED_TREESET))
                                                                .select(1)
                                                                .execute(
                                                                        new And.Builder()
                                                                                .select(0)
                                                                                .execute(new IsMoreThan(89L, true))
                                                                                .select(0)
                                                                                .execute(new IsLessThan(95L, true))
                                                                                .build())
                                                                .build())
                                                .build())
                                .build())
                .entity(TestGroups.ENTITY,
                        new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select("freqMap", ParquetStoreConstants.VERTEX)
                                                .execute(
                                                        new Or.Builder()
                                                                .select(0)
                                                                .execute(new Not<>(new IsEqual(TestUtils.MERGED_FREQMAP)))
                                                                .select(1)
                                                                .execute(new IsLessThan(2L, true))
                                                                .build())
                                                .build())
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
        for (long x = 0; x < 25; x++) {
            expected.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, false, (byte) 'a', 0.2 * x, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
            expected.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));

            expected.add(DataGen.getEdge(TestGroups.EDGE_2, x, x + 1, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEdge(TestGroups.EDGE_2, x, x + 1, false, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));

            expected.add(DataGen.getEntity(TestGroups.ENTITY, x, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEntity(TestGroups.ENTITY_2, x, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetSeededElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(48);
        final List<Element> actual = new ArrayList<>(48);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge(TestGroups.EDGE, 2L, 3L, true, (byte) 'b', 0.7, 6f, TestUtils.MERGED_TREESET, 17L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 5L, 6L, false, (byte) 'a', 1.0, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 5L, 6L, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 30L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 5L, 6L, true, (byte) 'b', 1.3, 6f, TestUtils.MERGED_TREESET, 35L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 10L, 11L, false, (byte) 'a', 2.0, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 10L, 11L, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 60L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 10L, 11L, true, (byte) 'b', 2.3, 6f, TestUtils.MERGED_TREESET, 65L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 13L, 14L, true, (byte) 'b', 2.9, 6f, TestUtils.MERGED_TREESET, 83L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, false, (byte) 'a', 3.0, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 90L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, true, (byte) 'b', 3.3, 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 2L, 3L, true, (byte) 'b', 0.7, 6f, TestUtils.MERGED_TREESET, 17L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 5L, 6L, false, (byte) 'b', 1.3, 6f, TestUtils.MERGED_TREESET, 35L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 5L, 6L, true, (byte) 'b', 1.3, 6f, TestUtils.MERGED_TREESET, 35L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 10L, 11L, false, (byte) 'b', 2.3, 6f, TestUtils.MERGED_TREESET, 65L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 10L, 11L, true, (byte) 'b', 2.3, 6f, TestUtils.MERGED_TREESET, 65L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 13L, 14L, true, (byte) 'b', 2.9, 6f, TestUtils.MERGED_TREESET, 83L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 15L, 16L, false, (byte) 'b', 3.3, 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 15L, 16L, true, (byte) 'b', 3.3, 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        expected.add(DataGen.getEntity(TestGroups.ENTITY, 2L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 22L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 3L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 33L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 5L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 55L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 10L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 110L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 13L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 143L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 14L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 154L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 15L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 165L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, 2L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 22L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, 3L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 33L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, 5L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 55L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, 10L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 110L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, 13L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 143L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, 14L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 154L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY_2, 15L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 165L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        expected.add(DataGen.getEdge(TestGroups.EDGE, 4L, 5L, false, (byte) 'a', 0.8, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 4L, 5L, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 24L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 4L, 5L, true, (byte) 'b', 1.1, 6f, TestUtils.MERGED_TREESET, 29L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 9L, 10L, false, (byte) 'a', 1.8, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 9L, 10L, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 54L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 9L, 10L, true, (byte) 'b', 2.1, 6f, TestUtils.MERGED_TREESET, 59L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, false, (byte) 'a', 2.8000000000000003, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 84L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, true, (byte) 'b', 3.1, 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 4L, 5L, false, (byte) 'b', 1.1, 6f, TestUtils.MERGED_TREESET, 29L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 4L, 5L, true, (byte) 'b', 1.1, 6f, TestUtils.MERGED_TREESET, 29L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 9L, 10L, false, (byte) 'b', 2.1, 6f, TestUtils.MERGED_TREESET, 59L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 9L, 10L, true, (byte) 'b', 2.1, 6f, TestUtils.MERGED_TREESET, 59L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 14L, 15L, false, (byte) 'b', 3.1, 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE_2, 14L, 15L, true, (byte) 'b', 3.1, 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(48);
        final List<Element> actual = new ArrayList<>(48);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, true, (byte) 'b', 3.3, 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, true, (byte) 'b', 3.1, 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        expected.add(DataGen.getEntity(TestGroups.ENTITY, 0L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 0L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 1L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 11L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEntity(TestGroups.ENTITY, 2L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 22L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetSeededAndFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(48);
        final List<Element> actual = new ArrayList<>(48);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, true, (byte) 'b', 3.3, 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        expected.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, true, (byte) 'b', 3.1, 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        expected.add(DataGen.getEntity(TestGroups.ENTITY, 2L, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 22L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }
}
