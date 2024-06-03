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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.AGE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.KNOWS;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MODERN_CONFIGURATION;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.SOFTWARE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.VADAS;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.WEIGHT;

/**
 * Verify behaviour against HasStep examples in the Tinkerpop HasStep documentation.
 * Needed since the HasStep optimisations were added in {@link GafferPopGraphStep}
 */
public class GafferPopHasStepIT {

    private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties.loadStoreProperties("/tinkerpop/map-store.properties");
    private static GraphTraversalSource g;

    @BeforeAll
    public static void beforeAll() {
        GafferPopGraph gafferPopGraph = GafferPopModernTestUtils.createModernGraph(GafferPopHasStepIT.class, MAP_STORE_PROPERTIES, MODERN_CONFIGURATION);
        g = gafferPopGraph.traversal();
    }

    @Test
    public void shouldFilterVerticesByLabel() {
        final List<Vertex> result = g.V().hasLabel(PERSON).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId(), JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterVerticesByLabelsOnly() {
        final List<Vertex> result = g.V().hasLabel(PERSON, SOFTWARE).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId(), JOSH.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    public void shouldThrowWhenFilterVerticesByInvalidLabels() {
        // Included this as it's an example from the docs that won't run due to Gaffer limitation
        // Gaffer checks labels in the view are valid before querying
        assertThatRuntimeException()
                .isThrownBy(() -> g.V().hasLabel(PERSON, NAME, MARKO.getName()).toList())
                .withMessageContaining("Entity group name does not exist in the schema")
                .withMessageContaining("Entity group marko does not exist in the schema");
        // 'should' just return all 'person' vertices
    }

    @Test
    public void shouldFilterVerticesByLabelAndProperty() {
        final List<Vertex> result = g.V().has(PERSON, NAME, MARKO.getName()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterVerticesByLabelAndPropertyLessThan() {
        final List<Vertex> result = g.V().has(AGE, P.lt(30))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), MARKO.getId());
    }

    @Test
    public void shouldFilterVerticesByLabelAndPropertyMoreThan() {
        final List<Vertex> result = g.V().has(PERSON, AGE, P.gt(30))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterVerticesByLabelAndPropertyWithin() {
        final List<Vertex> result = g.V().has(PERSON, NAME, P.within(VADAS.getName(), JOSH.getName()))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyInside() {
        final List<Object> result = g.V().has(PERSON, AGE, P.inside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(MARKO.getAge(), VADAS.getAge());
    }

    @Test
    public void shouldFilterVerticesByPropertyOutside() {
        final List<Object> result = g.V().has(PERSON, AGE, P.outside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(JOSH.getAge(), PETER.getAge());
    }

    @Test
    public void shouldFilterVerticesByPropertyWithin() {
        final List<Vertex> result = g.V().has(NAME, P.within(JOSH.getName(), MARKO.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), MARKO.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyWithout() {
        final List<Vertex> result = g.V().has(NAME, P.without(JOSH.getName(), MARKO.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyNotWithin() {
        final List<Vertex> result = g.V().has(NAME, P.not(P.within(JOSH.getName(), MARKO.getName()))).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyNot() {
        final List<Vertex> result = g.V().hasNot(AGE).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(RIPPLE.getId(), LOP.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyStartingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.startingWith("m")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyNotStartingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notStartingWith("m")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyEndingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.endingWith("o")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyNotEndingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notEndingWith("o")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId(), PETER.getId());
    }


    @Test
    public void shouldFilterVerticesByPropertyContaining() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.containing("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyNotContaining() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notContaining("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyRegex() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.regex("m.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterVerticesByPropertyNotRegex() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notRegex("m.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), VADAS.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterEdgesByLabel() {
        final List<Edge> result = g.E().hasLabel(KNOWS).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrderElementsOf(MARKO.knowsEdges());
    }

    @Test
    public void shouldThrowWhenFilterEdgesByInvalidLabels() {
        // Included this as it's an example from the docs that won't run due to Gaffer limitation
        // Gaffer checks labels in the view are valid before querying
        assertThatRuntimeException()
                .isThrownBy(() -> g.E().hasLabel(KNOWS, WEIGHT).toList())
                .withMessageContaining("Label/Group was not found in the schema: weight");
        // 'should' just return all 'knows' edges
    }

    @Test
    public void shouldFilterEdgesByPropertyInside() {
        final List<Edge> result = g.E().has(KNOWS, WEIGHT, P.inside(0.1, 0.6)).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactly(MARKO.knows(VADAS));
    }

    @Test
    public void shouldFilterEdgesByPropertyOutside() {
        final List<Edge> result = g.E().has(WEIGHT, P.outside(0.0, 0.9)).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(JOSH), JOSH.created(RIPPLE));
    }

    @Test
    public void shouldFilterEdgesByPropertyLessThan() {
        final List<Edge> result = g.E().has(WEIGHT, P.lt(0.4))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(PETER.created(LOP));
    }

    @Test
    public void shouldFilterEdgesByPropertyLessThanWithInvalidProperty() {
        // Uses fallback method due to Gremlin null error
        final List<Edge> result = g.E().has("invalid", P.lt(0.4))
                .toList();

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldHandleNulls() {
        final List<Long> result = g.V().has(AGE, P.within(27, null)).count().toList();
        assertThat(result).containsExactly(1L);
    }

    @Test
    public void shouldFilterById() {
        final List<Vertex> result = g.V().hasId(MARKO.getId(), JOSH.getId()).toList();
        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), JOSH.getId());
    }

    @Test
    public void shouldFilterByIdMissing() {
        final List<Vertex> result = g.V().hasId(new String[0]).toList();
        assertThat(result).isEmpty();
    }

}
