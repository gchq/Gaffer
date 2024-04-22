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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.stategy;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

class GafferPopGraphStepStrategyTest {

    private static final MapStoreProperties PROPERTIES = MapStoreProperties.loadStoreProperties(StreamUtil.openStream(
            GafferPopGraphStepStrategyTest.class, "/gaffer/map-store.properties"));

    @Test
    void shouldUpdateGraphVariablesOnGremlinWithStep() {
        // Given
        final GafferPopGraph graph = Mockito.spy(GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph()));
        // Need to stub the actual execution as it will reset the variables back to defaults after execution
        Mockito.doReturn(new ArrayList<>()).when(graph).execute(Mockito.any());
        final GafferPopGraphVariables graphVariables = (GafferPopGraphVariables) graph.variables();
        final String testUserId = "testUserId";
        final String testDataAuths = "auth1,auth2";
        final List<String> testOpOptions = Arrays.asList("graphId:graph1", "other:other");

        // Expected format variables are returned as once processed
        final String[] expectedDataAuths = {"auth1", "auth2"};
        final Map<String, String> expectedOpOptions = Stream.of(
                new SimpleEntry<>("graphId", "graph1"),
                new SimpleEntry<>("other", "other"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final GraphTraversalSource g = graph.traversal();

        // When
        g.with(GafferPopGraphVariables.USER_ID, testUserId)
            .with(GafferPopGraphVariables.DATA_AUTHS, testDataAuths)
            .with(GafferPopGraphVariables.OP_OPTIONS, testOpOptions)
            .V().toList();

        // Then
        assertThat(graphVariables.getUserId()).isEqualTo(testUserId);
        assertThat(graphVariables.getDataAuths()).isEqualTo(expectedDataAuths);
        assertThat(graphVariables.getOperationOptions()).isEqualTo(expectedOpOptions);
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


    private Graph getGafferGraph() {
        return GafferPopTestUtil.getGafferGraph(this.getClass(), PROPERTIES);
    }
}
