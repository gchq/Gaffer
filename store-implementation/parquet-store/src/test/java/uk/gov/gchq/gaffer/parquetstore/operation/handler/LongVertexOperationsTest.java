/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LongVertexOperationsTest extends AbstractOperationsTest {

    @Override
    public Schema createSchema() {
        return TestUtils.gafferSchema("schemaUsingLongVertexType");
    }

    @Override
    public List<Element> getInputDataForGetAllElementsTest() {
        return DataGen.generate300LongElements(false);
    }

    @Override
    public List<ElementSeed> getSeeds() {
        final List<ElementSeed> seeds = new ArrayList<>();
        seeds.add(new EntitySeed(5L));
        seeds.add(new EntitySeed(10L));
        seeds.add(new EntitySeed(15L));
        seeds.add(new EdgeSeed(13L, 14L, true));
        seeds.add(new EdgeSeed(2L, 3L, true));
        return seeds;
    }

    @Override
    protected List<ElementSeed> getSeedsThatWontAppear() {
        final List<ElementSeed> seeds = new ArrayList<>();
        seeds.add(new EntitySeed(-1L));
        seeds.add(new EntitySeed(300L));
        seeds.add(new EntitySeed(Long.MAX_VALUE));
        return seeds;
    }

    @Override
    protected View getView() {
        return new View.Builder()
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
                                                .select("freqMap", ParquetStore.VERTEX)
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
    public List<Element> getResultsForGetAllElementsTest() {
        final List<Element> results = new ArrayList<>();
        for (long x = 0; x < 25; x++) {
            results.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
            results.add(DataGen.getEntity(TestGroups.ENTITY, x, (byte) 'b', 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        }
        DataGen.generateBasicLongEntitys(TestGroups.ENTITY_2, 50, false)
                .stream()
                .forEach(results::add);
        StreamSupport
                .stream(getInputDataForGetAllElementsTest().spliterator(), false)
                .filter(e -> e.getGroup().equals(TestGroups.EDGE_2))
                .map(e -> (Edge) e)
                .forEach(results::add);
        return results;
    }

    @Override
    protected List<Element> getResultsForGetAllElementsWithViewTest() {
        final List<Element> results = new ArrayList<>();
        results.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 0L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 0L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 1L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 11L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 2L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 22L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        return results;
    }

    @Override
    protected List<Element> getResultsForGetAllElementsWithDirectedTypeTest() {
        return getResultsForGetAllElementsTest().stream()
                .filter(e -> e instanceof Entity || ((Edge) e).isDirected())
                .collect(Collectors.toList());
    }

    @Override
    protected List<Element> getResultsForGetAllElementsAfterTwoAdds() {
        final List<Element> results = new ArrayList<>();
        for (long x = 0; x < 25; x++) {
            results.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, true, (byte) 'b', 12f, TestUtils.MERGED_TREESET, 2L * ((6L * x) + 5L), (short) 26, TestUtils.DATE, TestUtils.DOUBLED_MERGED_FREQMAP, 4, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, false, (byte) 'a', 4f, TestUtils.getTreeSet1(), 2L * 5L, (short) 12, TestUtils.DATE, TestUtils.getDoubledFreqMap1(), 2, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, x, x + 1, false, (byte) 'b', 8f, TestUtils.getTreeSet2(), 2L * 6L * x, (short) 14, TestUtils.DATE1, TestUtils.getDoubledFreqMap2(), 2, null));
            results.add(DataGen.getEntity(TestGroups.ENTITY, x, (byte) 'b', 14f, TestUtils.MERGED_TREESET, 2L * ((5L * x) + (6L * x)), (short) 26, TestUtils.DATE, TestUtils.DOUBLED_MERGED_FREQMAP, 4, null));
        }
        // Elements for non-aggregating groups need to be added twice
        for (int i = 0; i < 2; i++) {
            getResultsForGetAllElementsTest().stream()
                    .filter(e -> e.getGroup().equals(TestGroups.ENTITY_2) || e.getGroup().equals(TestGroups.EDGE_2))
                    .forEach(results::add);
        }
        return results;
    }

    @Override
    public List<Element> getResultsForGetElementsWithSeedsRelatedTest() {
        final Set<Long> vertices = new HashSet<>();
        vertices.addAll(Lists.newArrayList(5L, 10L, 15L, 13L, 14L, 2L, 3L));
        final List<Element> results = new ArrayList<>();
        results.add(DataGen.getEdge(TestGroups.EDGE, 2L, 3L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 17L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 5L, 6L, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 5L, 6L, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 30L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 5L, 6L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 35L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 10L, 11L, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 10L, 11L, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 60L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 10L, 11L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 65L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 13L, 14L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 83L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 90L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 2L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 22L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 3L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 33L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 5L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 55L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 10L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 110L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 13L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 143L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 14L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 154L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 15L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 165L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        DataGen.generateBasicLongEntitys(TestGroups.ENTITY_2, 50, false)
                .stream()
                .filter(e -> vertices.contains(((Entity) e).getVertex()))
                .forEach(results::add);
        StreamSupport
                .stream(getInputDataForGetAllElementsTest().spliterator(), false)
                .filter(e -> e.getGroup().equals(TestGroups.EDGE_2))
                .map(e -> (Edge) e)
                .filter(e -> e.getSource().equals(5L) || e.getSource().equals(10L) || e.getSource().equals(15L)
                        || e.getDestination().equals(5L) || e.getDestination().equals(10L) || e.getDestination().equals(15L)
                        || (e.getSource().equals(13L) && e.getDestination().equals(14L) && e.isDirected())
                        || (e.getSource().equals(2L) && e.getDestination().equals(3L) && e.isDirected()))
                .forEach(results::add);
        results.add(DataGen.getEdge(TestGroups.EDGE, 4L, 5L, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 4L, 5L, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 24L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 4L, 5L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 29L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 9L, 10L, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 9L, 10L, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 54L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 9L, 10L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 59L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 84L, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsEqualTest() {
        final Set<Long> vertices = new HashSet<>();
        vertices.addAll(Lists.newArrayList(5L, 10L, 15L));
        final List<Element> results = new ArrayList<>();
        results.add(DataGen.getEdge(TestGroups.EDGE, 2L, 3L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 17L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 13L, 14L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 83L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 5L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 55L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 10L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 110L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 15L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 165L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        DataGen.generateBasicLongEntitys(TestGroups.ENTITY_2, 50, false)
                .stream()
                .filter(e -> vertices.contains(((Entity) e).getVertex()))
                .forEach(results::add);
        StreamSupport
                .stream(getInputDataForGetAllElementsTest().spliterator(), false)
                .filter(e -> e.getGroup().equals(TestGroups.EDGE_2))
                .map(e -> (Edge) e)
                .filter(e -> (e.getSource().equals(2L) && e.getDestination().equals(3L) && e.isDirected())
                        || (e.getSource().equals(13L) && e.getDestination().equals(14L) && e.isDirected()))
                .forEach(results::add);
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsAndViewTest() {
        final List<Element> results = new ArrayList<>();
        results.add(DataGen.getEdge(TestGroups.EDGE, 15L, 16L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, 14L, 15L, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 89L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY, 2L, (byte) 'b', 7f, TestUtils.MERGED_TREESET, 22L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithInOutTypeOutgoingTest() {
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .filter(e -> e.getVertex().equals(5L) || e.getVertex().equals(10L) || e.getVertex().equals(15L))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> e.isDirected())
                .filter(e -> e.getSource().equals(5L) || e.getSource().equals(10L) || e.getSource().equals(15L))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> !e.isDirected())
                .filter(e -> e.getSource().equals(5L) || e.getSource().equals(10L) || e.getSource().equals(15L)
                        || e.getDestination().equals(5L) || e.getDestination().equals(10L) || e.getDestination().equals(15L))
                .forEach(results::add);
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithInOutTypeIncomingTest() {
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .filter(e -> e.getVertex().equals(5L) || e.getVertex().equals(10L) || e.getVertex().equals(15L))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> e.isDirected())
                .filter(e -> e.getDestination().equals(5L) || e.getDestination().equals(10L) || e.getDestination().equals(15L))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> !e.isDirected())
                .filter(e -> e.getSource().equals(5L) || e.getSource().equals(10L) || e.getSource().equals(15L)
                        || e.getDestination().equals(5L) || e.getDestination().equals(10L) || e.getDestination().equals(15L))
                .forEach(results::add);
        return results;
    }

    @Override
    protected Edge getEdgeWithIdenticalSrcAndDst() {
        return DataGen.getEdge(TestGroups.EDGE_2, 100L, 100L, true, (byte) 'a', 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null);
    }
}
