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
import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TypeValueVertexOperationsTest extends AbstractOperationsTest {

    @Override
    protected Schema createSchema() {
        return TestUtils.gafferSchema("schemaUsingTypeValueVertexType");
    }

    @Override
    public List<Element> getInputDataForGetAllElementsTest() {
        return DataGen.generate300TypeValueElements(false);
    }

    @Override
    public List<ElementSeed> getSeeds() {
        final List<ElementSeed> seeds = new ArrayList<>();
        seeds.add(new EntitySeed(new TypeValue("type0", "vrt10")));
        seeds.add(new EntitySeed(new TypeValue("type2", "src17")));
        seeds.add(new EdgeSeed(new TypeValue("type1", "src11"), new TypeValue("type1", "dst12"), true));
        return seeds;
    }

    @Override
    protected List<ElementSeed> getSeedsThatWontAppear() {
        final List<ElementSeed> seeds = new ArrayList<>();
        seeds.add(new EntitySeed(new TypeValue("type0", "vrt10000")));
        seeds.add(new EntitySeed(new TypeValue("abc", "def")));
        seeds.add(new EdgeSeed(new TypeValue("abc", "def"), new TypeValue("type0", "src0"), true));
        return seeds;
    }

    @Override
    protected View getView() {
        return new View.Builder()
                .edge(TestGroups.EDGE,
                        new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select("float")
                                                .execute(
                                                        new IsMoreThan(3.0f, true))
                                                .build())
                                .build())
                .entity(TestGroups.ENTITY,
                        new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select(ParquetStore.VERTEX)
                                                .execute(
                                                        new Not<>(new IsEqual(new TypeValue("type0", "vrt10"))))
                                                .build())
                                .build())
                .build();
    }

    @Override
    public List<Element> getResultsForGetAllElementsTest() {
        final List<Element> results = new ArrayList<>();
        for (int x = 0; x < 25; x++) {
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            results.add(DataGen.getEdge(TestGroups.EDGE, src, dst, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
            results.add(DataGen.getEntity(TestGroups.ENTITY, vrt, (byte) 'b', 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        }
        DataGen.generateBasicTypeValueEntitys(TestGroups.ENTITY_2, 50, false)
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
        getResultsForGetAllElementsTest().stream()
                .filter(e -> e.getGroup().equals(TestGroups.EDGE))
                .map(e -> (Edge) e)
                .filter(e -> ((float) e.getProperty("float")) >= 3.0)
                .forEach(results::add);
        getResultsForGetAllElementsTest().stream()
                .filter(e -> e.getGroup().equals(TestGroups.ENTITY))
                .map(e -> (Entity) e)
                .filter(e -> !e.getVertex().equals(new TypeValue("type0", "vrt10")))
                .forEach(results::add);
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
        for (int x = 0; x < 25; x++) {
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            results.add(DataGen.getEdge(TestGroups.EDGE, src, dst, true, (byte) 'b', 12f, TestUtils.MERGED_TREESET, 2L * ((6L * x) + 5L), (short) 26, TestUtils.DATE, TestUtils.DOUBLED_MERGED_FREQMAP, 4, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'a', 4f, TestUtils.getTreeSet1(), 2L * 5L, (short) 12, TestUtils.DATE, TestUtils.getDoubledFreqMap1(), 2, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'b', 8f, TestUtils.getTreeSet2(), 2L * 6L * x, (short) 14, TestUtils.DATE1, TestUtils.getDoubledFreqMap2(), 2, null));
            results.add(DataGen.getEntity(TestGroups.ENTITY, vrt, (byte) 'b', 14f, TestUtils.MERGED_TREESET, 2L * ((5L * x) + (6L * x)), (short) 26, TestUtils.DATE, TestUtils.DOUBLED_MERGED_FREQMAP, 4, null));
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
        final List<Element> results = new ArrayList<>();
        // Results from type0-vrt10 seed
        final TypeValue t0v10 = new TypeValue("type0", "vrt10");
        results.add(DataGen.getEntity(TestGroups.ENTITY, t0v10, (byte) 'b', 7f, TestUtils.MERGED_TREESET, (5L * 10) + (6L * 10), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY_2, t0v10, (byte) 'a', 3f, TestUtils.getTreeSet1(), 5L * 10, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY_2, t0v10, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * 10, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, null));
        // Results from type2-src17 seed
        final TypeValue t2s17 = new TypeValue("type2", "src17");
        final TypeValue t2d18 = new TypeValue("type2", "dst18");
        results.add(DataGen.getEdge(TestGroups.EDGE, t2s17, t2d18, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 107L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, t2s17, t2d18, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * 17, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE, t2s17, t2d18, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t2s17, t2d18, true, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t2s17, t2d18, true, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * 17, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t2s17, t2d18, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t2s17, t2d18, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * 17, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, null));
        // Results from edge seed type1-src11, type1-dst12, true
        final TypeValue t1s11 = new TypeValue("type1", "src11");
        final TypeValue t1d12 = new TypeValue("type1", "dst12");
        results.add(DataGen.getEdge(TestGroups.EDGE, t1s11, t1d12, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 5L + 6L * 11, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t1s11, t1d12, true, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t1s11, t1d12, true, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * 11, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, null));
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsEqualTest() {
        final List<Element> results = new ArrayList<>();
        // Results from type0-vrt10 seed
        final TypeValue t0v10 = new TypeValue("type0", "vrt10");
        results.add(DataGen.getEntity(TestGroups.ENTITY, t0v10, (byte) 'b', 7f, TestUtils.MERGED_TREESET, (5L * 10) + (6L * 10), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY_2, t0v10, (byte) 'a', 3f, TestUtils.getTreeSet1(), 5L * 10, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEntity(TestGroups.ENTITY_2, t0v10, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * 10, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, null));
        // Results from type2-src17 seed
//        final TypeValue t2s17 = new TypeValue("type2", "src17");
//        final TypeValue t2d18 = new TypeValue("type2", "dst18");
        // Results from edge seed type1-src11, type1-dst12, true
        final TypeValue t1s11 = new TypeValue("type1", "src11");
        final TypeValue t1d12 = new TypeValue("type1", "dst12");
        results.add(DataGen.getEdge(TestGroups.EDGE, t1s11, t1d12, true, (byte) 'b', 6f, TestUtils.MERGED_TREESET, 5L + 6L * 11, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t1s11, t1d12, true, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        results.add(DataGen.getEdge(TestGroups.EDGE_2, t1s11, t1d12, true, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * 11, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, null));
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsAndViewTest() {
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e.getGroup().equals(TestGroups.EDGE))
                .map(e -> (Edge) e)
                .filter(e -> ((float) e.getProperty("float")) >= 3.0)
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e.getGroup().equals(TestGroups.ENTITY))
                .map(e -> (Entity) e)
                .filter(e -> !e.getVertex().equals(new TypeValue("type0", "vrt10")))
                .forEach(results::add);
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithInOutTypeOutgoingTest() {
        final TypeValue t0v10 = new TypeValue("type0", "vrt10");
        final TypeValue t2s17 = new TypeValue("type2", "src17");
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .filter(e -> e.getVertex().equals(t0v10) || e.getVertex().equals(t2s17))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> e.isDirected())
                .filter(e -> e.getSource().equals(t0v10) || e.getSource().equals(t2s17))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> !e.isDirected())
                .filter(e -> e.getSource().equals(t0v10) || e.getSource().equals(t2s17)
                        || e.getDestination().equals(t0v10) || e.getDestination().equals(t2s17))
                .forEach(results::add);
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithInOutTypeIncomingTest() {
        final TypeValue t0v10 = new TypeValue("type0", "vrt10");
        final TypeValue t2s17 = new TypeValue("type2", "src17");
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .filter(e -> e.getVertex().equals(t0v10) || e.getVertex().equals(t2s17))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> e.isDirected())
                .filter(e -> e.getDestination().equals(t0v10) || e.getDestination().equals(t2s17))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> !e.isDirected())
                .filter(e -> e.getSource().equals(t0v10) || e.getSource().equals(t2s17)
                        || e.getDestination().equals(t0v10) || e.getDestination().equals(t2s17))
                .forEach(results::add);
        return results;
    }

    @Override
    protected Edge getEdgeWithIdenticalSrcAndDst() {
        final TypeValue t0v10 = new TypeValue("type0", "vrt10");
        return DataGen.getEdge(TestGroups.EDGE_2, t0v10, t0v10, true, (byte) 'a', 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null);
    }
}
