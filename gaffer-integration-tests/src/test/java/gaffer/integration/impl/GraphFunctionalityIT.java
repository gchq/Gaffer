/*
 * Copyright 2016 Crown Copyright
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
package gaffer.integration.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.function.simple.filter.IsLessThan;
import gaffer.integration.GafferIntegrationTests;
import gaffer.integration.TraitRequirement;
import gaffer.integration.domain.EdgeDomainObject;
import gaffer.integration.generators.BasicEdgeGenerator;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.store.StoreTrait;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GraphFunctionalityIT extends GafferIntegrationTests {

    @Before
    public void data() throws OperationException {
        //BUILD
        GenerateElements<EdgeDomainObject> generateEdges = new GenerateElements<>(Arrays.asList(
                new EdgeDomainObject("A", "B", true, 1),
                new EdgeDomainObject("D", "A", true, 2),
                new EdgeDomainObject("B", "D", true, 3),
                new EdgeDomainObject("B", "C", true, 4),
                new EdgeDomainObject("B", "C", true, 5),
                new EdgeDomainObject("C", "B", true, 6),
                new EdgeDomainObject("C", "B", false, 7)
        ), new BasicEdgeGenerator());

        AddElements addElements = new AddElements();
        OperationChain<Void> opChain = new OperationChain.Builder()
                .first(generateEdges)
                .then(addElements)
                .build();

        graph.execute(opChain);
    }

    @Test
    public void testIncomingOutgoingDirectedEdges() throws OperationException {

        // OPERATE
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) (new EntitySeed("A")));
        final GetRelatedEdges getEdges = new GetRelatedEdges(seeds);
        getEdges.setIncludeIncomingOutGoing(GetOperation.IncludeIncomingOutgoingType.BOTH);
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "A", "B", true),
                new Edge(TestGroups.EDGE, "D", "A", true)
        ));
    }


    @Test
    public void testOutgoingEdgesIncludesUndirected() throws OperationException {

        // OPERATE
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) (new EntitySeed("C")));
        final GetRelatedEdges getEdges = new GetRelatedEdges(seeds);
        getEdges.setIncludeIncomingOutGoing(GetOperation.IncludeIncomingOutgoingType.OUTGOING);
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "C", "B", true),
                new Edge(TestGroups.EDGE, "C", "B", false)
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void testIdenticalEdgesAggregated() throws OperationException {
        // OPERATE
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) (new EntitySeed("B")));
        final GetRelatedEdges getEdges = new GetRelatedEdges(seeds);
        getEdges.setIncludeIncomingOutGoing(GetOperation.IncludeIncomingOutgoingType.OUTGOING);
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(3, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "B", "D", true),
                new Edge(TestGroups.EDGE, "B", "C", true),
                new Edge(TestGroups.EDGE, "C", "B", false)
        ));
        Edge aggregatedEdge = results.get(results.indexOf(new Edge(TestGroups.EDGE, "B", "C", true)));
        assertEquals(2L, aggregatedEdge.getProperty(TestPropertyNames.COUNT));
        assertEquals(9, aggregatedEdge.getProperty(TestPropertyNames.INT)); // 4 + 5

        Edge nonAggregatedEdge = results.get(results.indexOf(new Edge(TestGroups.EDGE, "B", "D", true)));
        assertEquals(1L, nonAggregatedEdge.getProperty(TestPropertyNames.COUNT));
        assertEquals(3, nonAggregatedEdge.getProperty(TestPropertyNames.INT));
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void testEdgesWithDifferentDirectionAreNotAggregated() throws OperationException {
        // OPERATE
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) (new EntitySeed("C")));
        final GetRelatedEdges getEdges = new GetRelatedEdges(seeds);
        getEdges.setIncludeIncomingOutGoing(GetOperation.IncludeIncomingOutgoingType.BOTH);
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(3, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "B", "C", true),
                new Edge(TestGroups.EDGE, "C", "B", true),
                new Edge(TestGroups.EDGE, "C", "B", false)
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.FILTERING)
    public void testFilteringProperties() throws OperationException {
        // BUILD
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewEdgeDefinition.Builder()
                        .property(TestPropertyNames.INT, Integer.class)
                        .filter(new ElementFilter.Builder()
                                .select(TestPropertyNames.INT)
                                .execute(new IsLessThan(4))
                                .build())
                        .build())
                .build();

        // OPERATE
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) (new EntitySeed("B")));
        final GetRelatedEdges getEdges = new GetRelatedEdges(view, seeds);
        getEdges.setIncludeIncomingOutGoing(GetOperation.IncludeIncomingOutgoingType.OUTGOING);
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(1, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "B", "D", true)
        ));
    }

    @Test
    @Ignore
    @TraitRequirement({StoreTrait.AGGREGATION, StoreTrait.FILTERING})
    public void testFilteringAggregatedPropertiesPartiallyMatches() throws OperationException {
        // BUILD
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewEdgeDefinition.Builder()
                        .property(TestPropertyNames.INT, Integer.class)
                        .filter(new ElementFilter.Builder()
                                .select(TestPropertyNames.INT)
                                .execute(new IsLessThan(5))
                                .build())
                        .build())
                .build();

        // OPERATE
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) (new EntitySeed("B")));
        final GetRelatedEdges getEdges = new GetRelatedEdges(view, seeds);
        getEdges.setIncludeIncomingOutGoing(GetOperation.IncludeIncomingOutgoingType.OUTGOING);
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "B", "D", true),
                new Edge(TestGroups.EDGE, "B", "C", true)
        ));
    }
}
