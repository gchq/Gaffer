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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.KNOWS;

class GafferPopVertexStepTest {

    private static Traversal.Admin<Vertex, Vertex> traversal;

    @BeforeAll
    public static void beforeAll() {
        GafferPopGraph modern = GafferPopModernTestUtils.createModernGraph(GafferPopVertexStepIT.class,
                StoreType.MAP);
        GraphTraversalSource g = modern.traversal();
        traversal = g.V().asAdmin();
    }

    @Test
    void shouldCheckEquality() {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.BOTH, KNOWS);
        GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep);

        Traversal.Admin<Vertex, Vertex> otherTraversal = traversal.clone();
        GafferPopVertexStep<Vertex> otherListStep = new GafferPopVertexStep<>(
                new VertexStep<>(otherTraversal, Vertex.class, Direction.BOTH, KNOWS));
        GafferPopVertexStep<Vertex> yetAnotherListStep = new GafferPopVertexStep<>(
                new VertexStep<>(otherTraversal, Vertex.class, Direction.BOTH));

        assertThat(listStep)
                .isEqualTo(otherListStep)
                .isNotEqualTo(null)
                .isNotEqualTo(vertexStep)
                .isNotEqualTo(yetAnotherListStep);
    }

    @Test
    void shouldConfigureParameters() throws Exception {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.BOTH, KNOWS);
        try (GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep)) {
            listStep.configure("key", "value");

            Parameters params = new Parameters();
            params.set(null, "key", "value");
            assertThat(listStep.getParameters()).hasSameHashCodeAs(params);
        }
    }

    @Test
    void shouldGetDirection() throws Exception {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.BOTH, KNOWS);
        try (GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep)) {
            assertThat(listStep.getDirection()).isEqualTo(Direction.BOTH);
        }
    }

    @Test
    void shouldReverseDirection() throws Exception {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.OUT, KNOWS);
        try (GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep)) {
            listStep.reverseDirection();
            assertThat(listStep.getDirection()).isEqualTo(Direction.IN);
        }
    }

    @Test
    void shouldReturnVertex() throws Exception {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.OUT, KNOWS);
        try (GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep)) {
            assertThat(listStep.returnsVertex()).isTrue();
            assertThat(listStep.returnsEdge()).isFalse();
        }
    }

    @Test
    void shouldHaveToString() throws Exception {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.OUT, KNOWS);
        try (GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep)) {
            assertThat(listStep).hasToString("GafferPopVertexStep(OUT,[knows],vertex)");
        }
    }

    @Test
    void shouldGetEdgeLabels() throws Exception {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.OUT, KNOWS);
        try (GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep)) {
            assertThat(listStep.getEdgeLabels()).containsExactly(KNOWS);
        }
    }

    @Test
    void shouldGetReturnClass() throws Exception {
        VertexStep<Vertex> vertexStep = new VertexStep<>(traversal, Vertex.class, Direction.OUT, KNOWS);
        try (GafferPopVertexStep<Vertex> listStep = new GafferPopVertexStep<>(vertexStep)) {
            assertThat(listStep.getReturnClass()).isEqualTo(Vertex.class);
        }
    }
}
