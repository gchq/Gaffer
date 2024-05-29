/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.generator;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

public class GafferEdgeGeneratorTest {
    @Test
    public void shouldConvertGafferPopEdgeToGafferEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String source = "source";
        final String dest = "dest";
        final String propValue = "property value";
        final GafferPopEdge gafferPopEdge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);
        gafferPopEdge.property(TestPropertyNames.STRING, propValue);
        when(graph.execute(Mockito.any())).thenReturn(new ArrayList<>());

        final GafferEdgeGenerator generator = new GafferEdgeGenerator();

        // When
        final Edge edge = generator._apply(gafferPopEdge);

        // Then
        assertThat(edge.getGroup()).isEqualTo(TestGroups.EDGE);
        assertThat(edge.getSource()).isEqualTo(source);
        assertThat(edge.getDestination()).isEqualTo(dest);
        assertThat(edge.isDirected()).isTrue();
        assertThat(edge.getProperties()).hasSize(1);
        assertThat(edge.getProperty(TestPropertyNames.STRING)).isEqualTo(propValue);
    }

}
