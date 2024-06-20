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

package uk.gov.gchq.gaffer.tinkerpop;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.KNOWS;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MODERN_CONFIGURATION;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.VADAS;

public class GafferPopGraphIT {
    private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties
            .loadStoreProperties("/tinkerpop/map-store.properties");
    private static GraphTraversalSource g;

    @BeforeAll
    public static void resetGraph() {
        GafferPopGraph gafferPopGraph = GafferPopModernTestUtils.createModernGraph(GafferPopGraphIT.class,
                MAP_STORE_PROPERTIES, MODERN_CONFIGURATION);
        g = gafferPopGraph.traversal();
    }

    @Test
    public void shouldGetAllVertices() {
        final List<Vertex> result = g.V().toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(
                        MARKO.getId(),
                        VADAS.getId(),
                        JOSH.getId(),
                        PETER.getId(),
                        LOP.getId(),
                        RIPPLE.getId());
    }

    @Test
    public void shouldTruncateGetAllVertices() {
        final List<Vertex> result = g.with("getAllElementsLimit", 2).V().toList();

        assertThat(result)
                .extracting(r -> r.id())
                .hasSize(2)
                .containsAnyOf(
                        MARKO.getId(),
                        VADAS.getId(),
                        JOSH.getId(),
                        PETER.getId(),
                        LOP.getId(),
                        RIPPLE.getId());
    }

    @Test
    public void shouldGetVerticesById() {
        final List<Vertex> result = g.V(MARKO.getId(), RIPPLE.getId()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), RIPPLE.getId());
    }

    @Test
    public void shouldGetVertex() {
        final List<Vertex> result = g.V(MARKO.getId()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldGetAllEdges() {
        final List<Edge> result = g.E().toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(
                        MARKO.knows(JOSH),
                        MARKO.knows(VADAS),
                        MARKO.created(LOP),
                        JOSH.created(LOP),
                        JOSH.created(RIPPLE),
                        PETER.created(LOP));
    }

    @Test
    public void shouldTruncateGetAllEdges() {
        final List<Edge> result = g.with("getAllElementsLimit", 2).E().toList();

        assertThat(result)
                .extracting(r -> r.id())
                .hasSize(2)
                .containsAnyOf(
                        MARKO.knows(JOSH),
                        MARKO.knows(VADAS),
                        MARKO.created(LOP),
                        JOSH.created(LOP),
                        JOSH.created(RIPPLE),
                        PETER.created(LOP));
    }

    @Test
    public void shouldGetEdgesById() {
        final List<Edge> result = g.E("[1,2]", "[4,3]").toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS), JOSH.created(LOP));
    }

    @Test
    public void shouldGetEdge() {
        final List<Edge> result = g.E("[1,2]").toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS));
    }

    @Test
    public void shouldAddV() {
        g.addV(PERSON).property(NAME, "stephen").property(T.id, "test").iterate();

        final List<Vertex> result = g.V().toList();
        assertThat(result)
                .extracting(r -> r.value(NAME))
                .contains("stephen");

        GafferPopGraphIT.resetGraph();
    }

    @Test
    public void shouldAddE() {
        g.addE(KNOWS).from(__.V(VADAS.getId())).to(__.V(PETER.getId())).iterate();

        final List<Edge> result = g.E().toList();
        assertThat(result)
                .extracting(r -> r.id())
                .contains(VADAS.knows(PETER));

        GafferPopGraphIT.resetGraph();
    }

}