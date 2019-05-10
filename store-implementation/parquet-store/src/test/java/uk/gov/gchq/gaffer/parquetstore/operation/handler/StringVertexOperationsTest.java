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
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StringVertexOperationsTest extends AbstractOperationsTest {

    @Override
    protected Schema createSchema() {
        return TestUtils.gafferSchema("schemaUsingStringVertexType");
    }

    @Override
    public List<Element> getInputDataForGetAllElementsTest() {
        return DataGen.generate300StringElementsWithNullProperties(false);
    }

    @Override
    public List<ElementSeed> getSeeds() {
        final List<ElementSeed> seeds = new ArrayList<>();
        seeds.add(new EntitySeed("vert10"));
        seeds.add(new EntitySeed("src5"));
        seeds.add(new EntitySeed("dst15"));
        seeds.add(new EdgeSeed("src13", "dst13", true));
        return seeds;
    }

    @Override
    protected List<ElementSeed> getSeedsThatWontAppear() {
        final List<ElementSeed> seeds = new ArrayList<>();
        seeds.add(new EntitySeed("vert100"));
        seeds.add(new EntitySeed("notpresentvert100"));
        seeds.add(new EntitySeed(""));
        return seeds;
    }

    @Override
    protected View getView() {
        return new View.Builder()
                .edge(TestGroups.EDGE,
                        new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select(ParquetStore.SOURCE)
                                                .execute(new Or<>(new IsLessThan("src12", true), new IsMoreThan("src4", true)))
                                                .build())
                                .build())
                .entity(TestGroups.ENTITY,
                        new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select(ParquetStore.VERTEX)
                                                .execute(
                                                        new Not<>(new IsMoreThan("vert12", false)))
                                                .build())
                                .build())
                .build();
    }

    @Override
    public List<Element> getResultsForGetAllElementsTest() {
        final List<Element> results = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            results.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, true, null, null, null, null, null, null, null, 2, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, false, null, null, null, null, null, null, null, 2, null));
            results.add(DataGen.getEntity(TestGroups.ENTITY, "vert" + i, null, null, null, null, null, null, null, 2, null));
        }
        DataGen.generateBasicStringEntitysWithNullProperties(TestGroups.ENTITY_2, 50, false)
                .stream()
                .forEach(results::add);
        getInputDataForGetAllElementsTest().stream()
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
                .filter(e -> ((String) e.getSource()).compareTo("src12") <= 0 || ((String) e.getSource()).compareTo("src4") >= 0)
                .forEach(results::add);
        getResultsForGetAllElementsTest().stream()
                .filter(e -> e.getGroup().equals(TestGroups.ENTITY))
                .map(e -> (Entity) e)
                .filter(e -> !(((String) e.getVertex()).compareTo("vert12") > 0))
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
        for (int i = 0; i < 25; i++) {
            results.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, true, null, null, null, null, null, null, null, 4, null));
            results.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, false, null, null, null, null, null, null, null, 4, null));
            results.add(DataGen.getEntity(TestGroups.ENTITY, "vert" + i, null, null, null, null, null, null, null, 4, null));
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
        // Results from vert10 seed
        results.add(DataGen.getEntity(TestGroups.ENTITY, "vert10", null, null, null, null, null, null, null, 2, null));
        for (int i = 0; i < 2; i++) {
            results.add(DataGen.getEntity(TestGroups.ENTITY_2, "vert10", null, null, null, null, null, null, null, 1, null));
        }
        // Results from src5 seed
        for (final boolean directed : Arrays.asList(true, false)) {
            results.add(DataGen.getEdge(TestGroups.EDGE, "src5", "dst5", directed, null, null, null, null, null, null, null, 2, null));
            for (int i = 0; i < 2; i++) {
                results.add(DataGen.getEdge(TestGroups.EDGE_2, "src5", "dst5", directed, null, null, null, null, null, null, null, 1, null));
            }
        }
        // Results from dst15 seed
        for (final boolean directed : Arrays.asList(true, false)) {
            results.add(DataGen.getEdge(TestGroups.EDGE, "src15", "dst15", directed, null, null, null, null, null, null, null, 2, null));
            for (int i = 0; i < 2; i++) {
                results.add(DataGen.getEdge(TestGroups.EDGE_2, "src15", "dst15", directed, null, null, null, null, null, null, null, 1, null));
            }
        }
        // Results from edge seed src13, dst13, true
        results.add(DataGen.getEdge(TestGroups.EDGE, "src13", "dst13", true, null, null, null, null, null, null, null, 2, null));
        for (int i = 0; i < 2; i++) {
            results.add(DataGen.getEdge(TestGroups.EDGE_2, "src13", "dst13", true, null, null, null, null, null, null, null, 1, null));
        }
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsEqualTest() {
        final List<Element> results = new ArrayList<>();
        // Results from vert10 seed
        results.add(DataGen.getEntity(TestGroups.ENTITY, "vert10", null, null, null, null, null, null, null, 2, null));
        for (int i = 0; i < 2; i++) {
            results.add(DataGen.getEntity(TestGroups.ENTITY_2, "vert10", null, null, null, null, null, null, null, 1, null));
        }
        // Results from edge seed src13, dst13, true
        results.add(DataGen.getEdge(TestGroups.EDGE, "src13", "dst13", true, null, null, null, null, null, null, null, 2, null));
        for (int i = 0; i < 2; i++) {
            results.add(DataGen.getEdge(TestGroups.EDGE_2, "src13", "dst13", true, null, null, null, null, null, null, null, 1, null));
        }
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsAndViewTest() {
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e.getGroup().equals(TestGroups.EDGE))
                .map(e -> (Edge) e)
                .filter(e -> ((String) e.getSource()).compareTo("src12") <= 0 || ((String) e.getSource()).compareTo("src4") >= 0)
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e.getGroup().equals(TestGroups.ENTITY))
                .map(e -> (Entity) e)
                .filter(e -> !(((String) e.getVertex()).compareTo("vert12") > 0))
                .forEach(results::add);
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithInOutTypeOutgoingTest() {
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .filter(e -> e.getVertex().equals("vert10") || e.getVertex().equals("src5") || e.getVertex().equals("dst15"))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> e.isDirected())
                .filter(e -> e.getSource().equals("vert10") || e.getSource().equals("src5") || e.getSource().equals("dst15"))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> !e.isDirected())
                .filter(e -> e.getSource().equals("vert10") || e.getSource().equals("src5") || e.getSource().equals("dst15")
                        || e.getDestination().equals("vert10") || e.getDestination().equals("src5") || e.getDestination().equals("dst15"))
                .forEach(results::add);
        return results;
    }

    @Override
    protected List<Element> getResultsForGetElementsWithInOutTypeIncomingTest() {
        final List<Element> results = new ArrayList<>();
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .filter(e -> e.getVertex().equals("vert10") || e.getVertex().equals("src5") || e.getVertex().equals("dst15"))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> e.isDirected())
                .filter(e -> e.getDestination().equals("vert10") || e.getDestination().equals("src5") || e.getDestination().equals("dst15"))
                .forEach(results::add);
        getResultsForGetElementsWithSeedsRelatedTest().stream()
                .filter(e -> e instanceof Edge)
                .map(e -> (Edge) e)
                .filter(e -> !e.isDirected())
                .filter(e -> e.getSource().equals("vert10") || e.getSource().equals("src5") || e.getSource().equals("dst15")
                        || e.getDestination().equals("vert10") || e.getDestination().equals("src5") || e.getDestination().equals("dst15"))
                .forEach(results::add);
        return results;
    }

    @Override
    protected Edge getEdgeWithIdenticalSrcAndDst() {
        return DataGen.getEdge(TestGroups.EDGE_2, "src", "src", true, (byte) 'a', 6f, TestUtils.MERGED_TREESET, 95L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null);
    }
}
