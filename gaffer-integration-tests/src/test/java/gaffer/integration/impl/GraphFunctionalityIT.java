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
import gaffer.data.element.Entity;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsLessThan;
import gaffer.integration.GafferIntegrationTests;
import gaffer.integration.TraitRequirement;
import gaffer.integration.domain.EdgeDomainObject;
import gaffer.integration.domain.EntityDomainObject;
import gaffer.integration.generators.BasicEdgeGenerator;
import gaffer.integration.generators.BasicEntityGenerator;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationChain.Builder;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.data.generator.EntitySeedExtractor;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.cache.FetchCache;
import gaffer.operation.impl.cache.FetchCachedResult;
import gaffer.operation.impl.cache.UpdateCache;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.store.StoreTrait;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

        GenerateElements<EntityDomainObject> generateEntities = new GenerateElements<>(Arrays.asList(
                new EntityDomainObject("A", "propA", 1),
                new EntityDomainObject("B", "propB", 2),
                new EntityDomainObject("C", "propC", 3),
                new EntityDomainObject("D", "propD", 4)
        ), new BasicEntityGenerator());

        OperationChain<Void> opChain = new OperationChain.Builder()
                .first(generateEdges)
                .then(new AddElements())
                .then(generateEntities)
                .then(new AddElements())
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
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
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
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
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

    @Test
    public void shouldCacheResults() throws OperationException {
        // OPERATE
        final Map<String, Iterable<?>> cachedResults = graph.execute(new Builder()
                .first(new GetRelatedEdges.Builder()
                        .addSeed(new EntitySeed("A"))
                        .build())
                .then(new UpdateCache())
                .then(new FetchCache())
                .build());

        //CHECK
        assertNotNull(cachedResults);
        assertEquals(1, cachedResults.size());
        assertThat(((Iterable<Edge>) cachedResults.get(UpdateCache.ALL)), IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "A", "B", true)
        ));
    }


    @Test
    public void shouldCacheResultsAndPassInputToOutput() throws OperationException {
        // OPERATE
        final Map<String, Iterable<?>> cachedResults = graph.execute(new Builder()
                .first(new GetRelatedEdges.Builder()
                        .addSeed(new EntitySeed("A"))
                        .build())
                .then(new UpdateCache("1"))
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor(IdentifierType.SOURCE))
                        .build())
                .then(new UpdateCache("2"))
                .then(new FetchCache())
                .build());

        //CHECK
        assertNotNull(cachedResults);
        assertEquals(2, cachedResults.size());
        assertThat(((Iterable<Edge>) cachedResults.get("1")), IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "A", "B", true),
                new Edge(TestGroups.EDGE, "D", "A", true)
        ));
        assertThat(((Iterable<EntitySeed>) cachedResults.get("2")), IsCollectionContaining.hasItems(
                new EntitySeed("A"),
                new EntitySeed("D")
        ));
    }

    @Test
    public void shouldCacheResultsSeparately() throws OperationException {
        // OPERATE
        final Map<String, Iterable<?>> cachedResults = graph.execute(new Builder()
                .first(new GetRelatedEdges.Builder()
                        .addSeed(new EntitySeed("A"))
                        .build())
                .then(new UpdateCache("1"))
                .then(new GetRelatedEdges.Builder()
                        .addSeed(new EntitySeed("B"))
                        .build())
                .then(new UpdateCache("2"))
                .then(new FetchCache())
                .build());

        //CHECK
        assertNotNull(cachedResults);
        assertEquals(2, cachedResults.size());
        assertThat(((Iterable<Edge>) cachedResults.get("1")), IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "A", "B", true),
                new Edge(TestGroups.EDGE, "D", "A", true)
        ));
        assertThat(((Iterable<Edge>) cachedResults.get("2")), IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "B", "D", true),
                new Edge(TestGroups.EDGE, "B", "C", true)
        ));
    }

    @Test
    public void shouldCacheResultsTogether() throws OperationException {
        // OPERATE
        final Map<String, Iterable<?>> cachedResults = graph.execute(new Builder()
                .first(new GetRelatedEdges.Builder()
                        .addSeed(new EntitySeed("A"))
                        .build())
                .then(new UpdateCache())
                .then(new GetRelatedEdges.Builder()
                        .addSeed(new EntitySeed("B"))
                        .build())
                .then(new UpdateCache())
                .then(new FetchCache())
                .build());

        //CHECK
        assertNotNull(cachedResults);
        assertEquals(1, cachedResults.size());
        assertThat(((Iterable<Edge>) cachedResults.get(UpdateCache.ALL)), IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "A", "B", true),
                new Edge(TestGroups.EDGE, "D", "A", true),
                new Edge(TestGroups.EDGE, "B", "D", true),
                new Edge(TestGroups.EDGE, "B", "C", true)
        ));
    }

    @Test
    public void shouldCacheResultsAndFetchThemInChain() throws OperationException {
        // OPERATE
        final Map<String, Iterable<?>> cachedResults = graph.execute(new Builder()
                .first(new GetAdjacentEntitySeeds.Builder()
                        .addSeed(new EntitySeed("A"))
                        .build())
                .then(new UpdateCache("adj-seeds"))
                .then(new GetEntitiesBySeed())
                .then(new UpdateCache("entities"))
                .then(new FetchCachedResult("adj-seeds"))
                .then(new GetRelatedEdges())
                .then(new UpdateCache("edges"))
                .then(new FetchCache())
                .build());

        //CHECK
        assertNotNull(cachedResults);
        assertEquals(3, cachedResults.size());
        assertThat(((Iterable<EntitySeed>) cachedResults.get("adj-seeds")), IsCollectionContaining.hasItems(
                new EntitySeed("B"),
                new EntitySeed("D")
        ));
        assertThat(((Iterable<Entity>) cachedResults.get("entities")), IsCollectionContaining.hasItems(
                new Entity(TestGroups.ENTITY, "B"),
                new Entity(TestGroups.ENTITY, "D")
        ));
        assertThat(((Iterable<Edge>) cachedResults.get("edges")), IsCollectionContaining.hasItems(
                new Edge(TestGroups.EDGE, "B", "C", true),
                new Edge(TestGroups.EDGE, "B", "D", true),
                new Edge(TestGroups.EDGE, "A", "B", true),
                new Edge(TestGroups.EDGE, "C", "B", true),
                new Edge(TestGroups.EDGE, "B", "C", true),
                new Edge(TestGroups.EDGE, "D", "A", true),
                new Edge(TestGroups.EDGE, "B", "D", true)
        ));
    }

    @Test
    public void shouldCacheResultsTogetherWhilstTraversing() throws OperationException {
        // OPERATE
        final Iterable<?> cachedResults = graph.execute(new Builder()
                .first(new GetAdjacentEntitySeeds.Builder()
                        .addSeed(new EntitySeed("A"))
                        .build())
                .then(new UpdateCache())
                .then(new GetAdjacentEntitySeeds.Builder()
                        .build())
                .then(new UpdateCache())
                .then(new FetchCachedResult())
                .build());

        //CHECK
        assertNotNull(cachedResults);
        final List<EntitySeed> results = (List) Lists.newArrayList(cachedResults);
        assertThat(results, IsCollectionContaining.hasItems(
                new EntitySeed("A"),
                new EntitySeed("B"),
                new EntitySeed("C"),
                new EntitySeed("D")
        ));
    }
}
