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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.OTHER_TSTV_PROPERTY_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_ID_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_PROPERTY_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.AGE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.KNOWS;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.SOFTWARE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.VADAS;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.WEIGHT;

/**
 * Verify behaviour against HasStep examples in the Tinkerpop HasStep documentation.
 * Needed since the HasStep optimisations were added in {@link GafferPopGraphStep}
 *
 * Tests filtering that has been incorporated into a GafferPopGraphStep
 * e.g.
 * <code>g.V().has(...)</code>
 * not
 * <code>g.V().out().has(...)</code>
 */
class GafferPopGraphStepIT {

    private static GraphTraversalSource g;
    private static GraphTraversalSource tstvG;

    @BeforeAll
    public static void beforeAll() {
        GafferPopGraph modern = GafferPopModernTestUtils.createModernGraph(GafferPopGraphStepIT.class, StoreType.ACCUMULO);
        g = modern.traversal();

        GafferPopGraph tstv = GafferPopTstvTestUtils.createTstvGraph();
        tstvG = tstv.traversal();
    }

    @Test
    void shouldGetVertexBySeed() {
        final List<Vertex> result = g.V(MARKO.getId()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    void shouldGetVertexByTSTVSeed() {
        final List<Vertex> result = tstvG.V(TSTV_ID_STRING).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(TSTV_ID_STRING);
    }


    @Test
    void shouldFilterVerticesByLabel() {
        final List<Vertex> result = g.V().hasLabel(PERSON).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId(), JOSH.getId(), PETER.getId());
    }

    @Test
    void shouldFilterVerticesByLabelsOnly() {
        final List<Vertex> result = g.V().hasLabel(PERSON, SOFTWARE).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId(), JOSH.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    void shouldThrowWhenFilterVerticesByInvalidLabels() {
        // Included this as it's an example from the docs that won't run due to Gaffer limitation
        // Gaffer checks labels in the view are valid before querying
        assertThatRuntimeException()
                .isThrownBy(() -> g.V().hasLabel(PERSON, NAME, MARKO.getName()).toList())
                .withMessageContaining("Entity group name does not exist in the schema")
                .withMessageContaining("Entity group marko does not exist in the schema");
        // 'should' just return all 'person' vertices
    }

    @Test
    void shouldFilterVerticesByLabelAndProperty() {
        final List<Vertex> result = g.V().has(PERSON, NAME, MARKO.getName()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvProperty() {
        final List<Vertex> result = tstvG.V().has(TSTV, NAME, TSTV_PROPERTY_STRING).toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByLabelAndPropertyLessThan() {
        final List<Vertex> result = g.V().has(AGE, P.lt(30))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), MARKO.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvPropertyLessThan() {
        final List<Vertex> result = tstvG.V().has(NAME, P.lt(OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByLabelAndPropertyMoreThan() {
        final List<Vertex> result = g.V().has(PERSON, AGE, P.gt(30))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), PETER.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvPropertyMoreThan() {
        final List<Vertex> result = tstvG.V().has(NAME, P.gt(TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(OTHER_TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByLabelAndPropertyWithin() {
        final List<Vertex> result = g.V().has(PERSON, NAME, P.within(VADAS.getName(), JOSH.getName()))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvPropertyWithin() {
        final List<Vertex> result = tstvG.V().has(TSTV, NAME, P.within(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(OTHER_TSTV_PROPERTY_STRING, TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByPropertyInside() {
        final List<Object> result = g.V().has(PERSON, AGE, P.inside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(MARKO.getAge(), VADAS.getAge());
    }

    @Test
    void shouldFilterVerticesByPropertyOutside() {
        final List<Object> result = g.V().has(PERSON, AGE, P.outside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(JOSH.getAge(), PETER.getAge());
    }

    @Test
    void shouldFilterVerticesByPropertyWithin() {
        final List<Vertex> result = g.V().has(NAME, P.within(JOSH.getName(), MARKO.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), MARKO.getId());
    }

    @Test
    void shouldFilterVerticesByTstvPropertyWithin() {
        final List<Vertex> result = tstvG.V().has(NAME, P.within(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(OTHER_TSTV_PROPERTY_STRING, TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByPropertyWithout() {
        final List<Vertex> result = g.V().has(NAME, P.without(JOSH.getName(), MARKO.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    void shouldFilterVerticesByTstvPropertyWithout() {
        final List<Vertex> result = tstvG.V().has(NAME, P.without(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result).isEmpty();
    }

    @Test
    void shouldFilterVerticesByPropertyNotWithin() {
        final List<Vertex> result = g.V().has(NAME, P.not(P.within(JOSH.getName(), MARKO.getName()))).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    void shouldFilterVerticesByTstvPropertyNotWithin() {
        final List<Vertex> result = tstvG.V().has(NAME, P.not(P.within(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING)))
                .toList();

        assertThat(result).isEmpty();
    }

    @Test
    void shouldFilterVerticesByPropertyNot() {
        final List<Vertex> result = g.V().hasNot(AGE).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(RIPPLE.getId(), LOP.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyStartingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.startingWith("m")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotStartingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notStartingWith("m")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId(), PETER.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyEndingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.endingWith("o")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotEndingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notEndingWith("o")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId(), PETER.getId());
    }


    @Test
    void shouldFilterVerticesByPropertyContaining() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.containing("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotContaining() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notContaining("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), PETER.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyRegex() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.regex("m.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotRegex() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notRegex("m.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), VADAS.getId(), PETER.getId());
    }

    @Test
    void shouldFilterEdgesByLabel() {
        final List<Edge> result = g.E().hasLabel(KNOWS).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrderElementsOf(MARKO.knowsEdges());
    }

    @Test
    void shouldGetEdgesByIdAndLabelThenFilterByLabel() {
        final List<Edge> result = g.E("[1, knows, 2]").hasLabel("knows").toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS));
    }

    @Test
    void shouldGetEdgesByIdAndLabelThenFilterById() {
        final List<Edge> result = g.E("[4, created, 3]").outV().outE().has(T.id, "[4, created, 5]").toList();

        assertThat(result)
            .extracting(r -> r.id())
            .containsExactly(JOSH.created(RIPPLE));
    }

    @Test
    void shouldThrowWhenFilterEdgesByInvalidLabels() {
        // Included this as it's an example from the docs that won't run due to Gaffer limitation
        // Gaffer checks labels in the view are valid before querying
        assertThatRuntimeException()
                .isThrownBy(() -> g.E().hasLabel(KNOWS, WEIGHT).toList())
                .withMessageContaining("Label/Group was not found in the schema: weight");
        // 'should' just return all 'knows' edges
    }

    @Test
    void shouldFilterEdgesByPropertyInside() {
        final List<Edge> result = g.E().has(KNOWS, WEIGHT, P.inside(0.1, 0.6)).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactly(MARKO.knows(VADAS));
    }

    @Test
    void shouldFilterEdgesByPropertyOutside() {
        final List<Edge> result = g.E().has(WEIGHT, P.outside(0.0, 0.9)).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(JOSH), JOSH.created(RIPPLE));
    }

    @Test
    void shouldFilterEdgesByPropertyLessThan() {
        final List<Edge> result = g.E().has(WEIGHT, P.lt(0.4))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(PETER.created(LOP));
    }

    @Test
    void shouldFilterEdgesByPropertyLessThanWithInvalidProperty() {
        // Uses fallback method due to Gremlin null error
        final List<Edge> result = g.E().has("invalid", P.lt(0.4))
                .toList();

        assertThat(result).isEmpty();
    }

    @Test
    void shouldHandleNulls() {
        final List<Long> result = g.V().has(AGE, P.within(27, null)).count().toList();
        assertThat(result).containsExactly(1L);
    }

    @Test
    void shouldFilterVertexById() {
        final List<Vertex> result = g.V().hasId(MARKO.getId(), JOSH.getId()).toList();
        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), JOSH.getId());
    }

    @Test
    void shouldFilterVertexByIdMissing() {
        final List<Vertex> result = g.V().hasId(new String[0]).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void shouldFilterEdgeById() {
        final List<Edge> result = g.E().hasId("[1,2]").toList();
        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS));
    }

    @Test
    void shouldFilterEdgeByIdMissing() {
        final List<Edge> result = g.E().hasId(new String[0]).toList();
        assertThat(result).isEmpty();
    }

}
