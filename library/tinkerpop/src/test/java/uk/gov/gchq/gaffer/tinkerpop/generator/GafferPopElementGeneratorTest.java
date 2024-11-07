/*
 * Copyright 2023-2024 Crown Copyright
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopElement;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;

class GafferPopElementGeneratorTest {

    @Test
    void shouldReturnAGafferPopVertex() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Element element = new Entity.Builder().group(TestGroups.ENTITY).build();
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(graph, true);

        // When
        final GafferPopElement gafferPopElement = generator._apply(element);

        // Then
        assertThat(gafferPopElement).isInstanceOf(GafferPopVertex.class);
        assertThat(gafferPopElement.label()).isEqualTo(TestGroups.ENTITY);
        assertThat(gafferPopElement.graph()).isSameAs(graph);
    }

    @Test
    void shouldReturnAGafferPopEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final String source = "source";
        final String dest = "dest";
        final Element element = new Edge.Builder()
            .group(TestGroups.EDGE)
            .source(source)
            .dest(dest).build();

        final GafferPopElementGenerator generator = new GafferPopElementGenerator(graph, true);

        // When
        final GafferPopElement gafferPopElement = generator._apply(element);

        // Then
        assertThat(gafferPopElement).isInstanceOf(GafferPopEdge.class);
        assertThat(gafferPopElement.label()).isEqualTo(TestGroups.EDGE);
        assertThat(gafferPopElement.graph()).isSameAs(graph);
    }

    @Test
    void shouldThrowExceptionForInvalidElement() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Element element = mock(Element.class);
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(graph, true);

        // Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> generator._apply(element));
    }
}
