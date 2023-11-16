/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphFeatures.GafferPopGraphEdgeFeatures;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphFeatures.GafferPopGraphGraphFeatures;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphFeatures.GafferPopGraphVertexFeatures;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphFeatures.GafferPopGraphVertexPropertyFeatures;

import java.util.UUID;

public class GafferPopGraphFeaturesTest {
    public static final String STRING_ID = "testId";
    public static final Integer INT_ID = 1;
    public static final UUID UUID_ID = UUID.randomUUID();
    final GafferPopGraph graph = mock(GafferPopGraph.class);
    final Features features = new GafferPopGraphFeatures();

    @Test
    public void shouldHaveCertainGafferPopGraphGraphFeatures() {
        given(graph.features()).willReturn(features);
        final Features graphFeatures = graph.features();

        assertThat(graphFeatures.graph()).isInstanceOf(GafferPopGraphGraphFeatures.class);
        assertThat(graphFeatures.graph().supportsTransactions()).isFalse();
        assertThat(graphFeatures.graph().supportsThreadedTransactions()).isFalse();
        assertThat(graphFeatures.graph().supportsComputer()).isFalse();
    }

    @Test
    public void shouldHaveCertainGafferPopGraphVertexFeatures() {
        given(graph.features()).willReturn(features);

        final Features vertexFeatures = graph.features();

        assertThat(vertexFeatures.vertex()).isInstanceOf(GafferPopGraphVertexFeatures.class);
        assertThat(vertexFeatures.vertex().supportsRemoveVertices()).isFalse();
        assertThat(vertexFeatures.vertex().supportsRemoveProperty()).isFalse();
        assertThat(vertexFeatures.vertex().willAllowId(STRING_ID)).isTrue();
        assertThat(vertexFeatures.vertex().willAllowId(INT_ID)).isTrue();
        assertThat(vertexFeatures.vertex().willAllowId(UUID_ID)).isTrue();
        assertThat(vertexFeatures.vertex().willAllowId(null)).isFalse();
    }

    @Test
    public void shouldHaveCertainGafferPopGraphEdgeFeatures() {
        given(graph.features()).willReturn(features);

        final Features edgeFeatures = graph.features();

        assertThat(edgeFeatures.edge()).isInstanceOf(GafferPopGraphEdgeFeatures.class);
        assertThat(edgeFeatures.edge().supportsRemoveEdges()).isFalse();
        assertThat(edgeFeatures.edge().supportsRemoveProperty()).isFalse();
        assertThat(edgeFeatures.edge().willAllowId(STRING_ID)).isTrue();
        assertThat(edgeFeatures.vertex().willAllowId(INT_ID)).isTrue();
        assertThat(edgeFeatures.vertex().willAllowId(UUID_ID)).isTrue();
        assertThat(edgeFeatures.vertex().willAllowId(null)).isFalse();
    }

    @Test
    public void shouldReturnStringOfFeatures() {
        given(graph.features()).willReturn(features);

        assertThat(graph.features().toString()).contains("FEATURES", "GraphFeatures", "VariableFeatures",
            "VertexFeatures", "VertexPropertyFeatures", "EdgeFeatures", "EdgePropertyFeatures");
    }

    @Test
    public void shouldHaveCertainGafferPopGraphVertexPropertyFeatures() {
        given(graph.features()).willReturn(features);

        final Features.VertexPropertyFeatures vertexPropertyFeatures = graph.features().vertex().properties();

        assertThat(vertexPropertyFeatures).isInstanceOf(GafferPopGraphVertexPropertyFeatures.class);
        assertThat(vertexPropertyFeatures.supportsRemoveProperty()).isFalse();
        assertThat(vertexPropertyFeatures.willAllowId(STRING_ID)).isTrue();
        assertThat(vertexPropertyFeatures.willAllowId(INT_ID)).isTrue();
        assertThat(vertexPropertyFeatures.willAllowId(UUID_ID)).isTrue();
        assertThat(vertexPropertyFeatures.willAllowId(null)).isFalse();
    }
}
