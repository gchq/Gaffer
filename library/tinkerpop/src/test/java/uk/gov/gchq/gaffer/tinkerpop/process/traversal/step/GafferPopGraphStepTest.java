/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.verify;

class GafferPopGraphStepTest {

    private static final MapStoreProperties PROPERTIES = MapStoreProperties.loadStoreProperties(StreamUtil.openStream(
            GafferPopGraphStepTest.class, "/gaffer/map-store.properties"));

    @Test
    void shouldUpdateGraphVariablesOnGremlinWithStep() {
        // Given
        final GafferPopGraph graph = GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph());
        final GafferPopGraphVariables graphVariables = (GafferPopGraphVariables) graph.variables();
        final List<String> testOpOptions = Arrays.asList("graphId:graph1", "other:other");

        final GraphTraversalSource g = graph.traversal();

        // When
        g.with(GafferPopGraphVariables.OP_OPTIONS, testOpOptions).V().toList();

        // Then
        assertThat(graphVariables.getOperationOptions()).containsOnly(
            entry("graphId", "graph1"),
            entry("other", "other"));
    }

    @Test
    void shouldUseViewWhenHasLabelIsRequested() {
        // Given
        final GafferPopGraph graph = Mockito.spy(GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph()));
        final GraphTraversalSource g = graph.traversal();
        final String entityGroup = "software";
        final String edgeGroup = "created";
        final View entityView = new View.Builder()
            .entity(entityGroup)
            .build();
        final View edgeView = new View.Builder()
            .edge(edgeGroup)
            .build();

        // When
        g.V().hasLabel(entityGroup).toList();
        g.E().hasLabel(edgeGroup).toList();

        // Then
        verify(graph, Mockito.atLeastOnce()).vertices(Mockito.any(), Mockito.eq(entityGroup));
        verify(graph, Mockito.atLeastOnce()).verticesWithView(Mockito.any(), Mockito.eq(entityView));

        verify(graph, Mockito.atLeastOnce()).edges(Mockito.any(), Mockito.eq(Direction.BOTH), Mockito.eq(edgeGroup));
        verify(graph, Mockito.atLeastOnce()).edgesWithView(Mockito.any(), Mockito.eq(Direction.BOTH), Mockito.eq(edgeView));
    }

    @Test
    void shouldFilterPostAgg() {
        // Given
        final GafferPopGraph graph = Mockito.spy(GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph()));
        final GraphTraversalSource g = graph.traversal();
        final String entityGroup = "software";

        // When
        g.with("hasStepFilterStage", "POST_AGGREGATION").V().has(entityGroup, "prop", "value").toList();

        View expected = new View.Builder()
            .entity(entityGroup, new ViewElementDefinition.Builder()
                .postAggregationFilter(new ElementFilter.Builder()
                    .select("prop")
                    .execute(new Exists())
                    .select("prop")
                    .execute(new IsEqual("value"))
                    .build())
                .build())
            .build();

        // Then
        verify(graph, Mockito.atLeastOnce())
            .verticesWithView(Mockito.any(), Mockito.eq(expected));
    }

    @Test
    void shouldFilterPostTransform() {
        // Given
        final GafferPopGraph graph = Mockito.spy(GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph()));
        final GraphTraversalSource g = graph.traversal();
        final String entityGroup = "software";

        // When
        g.with("hasStepFilterStage", "POST_TRANSFORM").V().has(entityGroup, "prop", "value").toList();

        View expected = new View.Builder()
            .entity(entityGroup, new ViewElementDefinition.Builder()
                .postTransformFilter(new ElementFilter.Builder()
                    .select("prop")
                    .execute(new Exists())
                    .select("prop")
                    .execute(new IsEqual("value"))
                    .build())
                .build())
            .build();

        // Then
        verify(graph, Mockito.atLeastOnce())
            .verticesWithView(Mockito.any(), Mockito.eq(expected));
    }

    @Test
    void shouldFilterPreAggregation() {
        // Given
        final GafferPopGraph graph = Mockito.spy(GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph()));
        final GraphTraversalSource g = graph.traversal();
        final String entityGroup = "software";

        // When
        g.with("hasStepFilterStage", "PRE_AGGREGATION").V().has(entityGroup, "prop", "value").toList();

        View expected = new View.Builder()
            .entity(entityGroup, new ViewElementDefinition.Builder()
                .preAggregationFilter(new ElementFilter.Builder()
                    .select("prop")
                    .execute(new Exists())
                    .select("prop")
                    .execute(new IsEqual("value"))
                    .build())
                .build())
            .build();

        // Then
        verify(graph, Mockito.atLeastOnce())
            .verticesWithView(Mockito.any(), Mockito.eq(expected));
    }

    @Test
    void shouldFilterPreAggregationWhenValueInvalid() {
        // Given
        final GafferPopGraph graph = Mockito.spy(GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph()));
        final GraphTraversalSource g = graph.traversal();
        final String entityGroup = "software";

        // When
        g.with("hasStepFilterStage", "invalid").V().has(entityGroup, "prop", "value").toList();

        View expected = new View.Builder()
            .entity(entityGroup, new ViewElementDefinition.Builder()
                .preAggregationFilter(new ElementFilter.Builder()
                    .select("prop")
                    .execute(new Exists())
                    .select("prop")
                    .execute(new IsEqual("value"))
                    .build())
                .build())
            .build();

        // Then
        verify(graph, Mockito.atLeastOnce())
            .verticesWithView(Mockito.any(), Mockito.eq(expected));
    }

    @Test
    void shouldFilterPreAggregationWhenNotSpecified() {
        // Given
        final GafferPopGraph graph = Mockito.spy(GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph()));
        final GraphTraversalSource g = graph.traversal();
        final String entityGroup = "software";

        // When
        g.V().has(entityGroup, "prop", "value").toList();

        View expected = new View.Builder()
            .entity(entityGroup, new ViewElementDefinition.Builder()
                .preAggregationFilter(new ElementFilter.Builder()
                    .select("prop")
                    .execute(new Exists())
                    .select("prop")
                    .execute(new IsEqual("value"))
                    .build())
                .build())
            .build();

        // Then
        verify(graph, Mockito.atLeastOnce())
            .verticesWithView(Mockito.any(), Mockito.eq(expected));
    }


    private Graph getGafferGraph() {
        return GafferPopTestUtil.getGafferGraph(this.getClass(), PROPERTIES);
    }
}
