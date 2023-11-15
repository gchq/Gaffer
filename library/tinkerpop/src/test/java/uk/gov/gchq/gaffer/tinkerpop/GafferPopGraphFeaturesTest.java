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

public class GafferPopGraphFeaturesTest {
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
        final Object id = "testId";
        given(graph.features()).willReturn(features);

        final Features vertexFeatures = graph.features();

        assertThat(vertexFeatures.vertex()).isInstanceOf(GafferPopGraphVertexFeatures.class);
        assertThat(vertexFeatures.vertex().supportsRemoveVertices()).isFalse();
        assertThat(vertexFeatures.vertex().supportsRemoveProperty()).isFalse();
        assertThat(vertexFeatures.vertex().willAllowId(id)).isTrue();
    }

    @Test
    public void shouldHaveCertainGafferPopGraphEdgeFeatures() {
        final Object id = "testId";
        given(graph.features()).willReturn(features);

        final Features edgeFeatures = graph.features();

        assertThat(edgeFeatures.edge()).isInstanceOf(GafferPopGraphEdgeFeatures.class);
        assertThat(edgeFeatures.edge().supportsRemoveEdges()).isFalse();
        assertThat(edgeFeatures.edge().supportsRemoveProperty()).isFalse();
        assertThat(edgeFeatures.edge().willAllowId(id)).isTrue();
    }

    @Test
    public void shouldReturnStringOfFeatures() {
        given(graph.features()).willReturn(features);

        assertThat(graph.features().toString()).contains("FEATURES", "ServiceCall");
    }

    @Test
    public void shouldHaveCertainGafferPopGraphVertexPropertyFeatures() {
        final Object id = "testId";
        given(graph.features()).willReturn(features);

        final Features.VertexPropertyFeatures vertexPropertyFeatures = graph.features().vertex().properties();

        assertThat(vertexPropertyFeatures).isInstanceOf(GafferPopGraphVertexPropertyFeatures.class);
        assertThat(vertexPropertyFeatures.supportsRemoveProperty()).isFalse();
        assertThat(vertexPropertyFeatures.willAllowId(id)).isTrue();
    }
}
