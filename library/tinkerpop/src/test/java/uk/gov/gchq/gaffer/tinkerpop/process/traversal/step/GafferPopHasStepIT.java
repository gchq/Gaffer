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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.OTHER_TSTV_PROPERTY_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_PROPERTY_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.AGE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.VADAS;

/**
 * Runs all tests against <code>g.V().out()</code> so that a HasStep is
 * used rather than scooped into the GafferPopGraphStep
 * For testing has filtering when applied to a GraphStep see {@link GafferPopGraphStepIT}
 */
class GafferPopHasStepIT {

    private static GraphTraversalSource g;
    private static GraphTraversalSource tstvG;

    @BeforeAll
    public static void beforeAll() {
        GafferPopGraph modern = GafferPopModernTestUtils.createModernGraph(GafferPopHasStepIT.class, StoreType.ACCUMULO);
        g = modern.traversal();

        GafferPopGraph tstv = GafferPopTstvTestUtils.createTstvGraph();
        tstvG = tstv.traversal();
    }


    @Test
    void shouldFilterVerticesByLabelAndProperty() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, JOSH.getName()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvProperty() {
        final List<Vertex> result = tstvG.V().out().has(TSTV, NAME, TSTV_PROPERTY_STRING).toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByLabelAndPropertyLessThan() {
        final List<Vertex> result = g.V().out().has(AGE, P.lt(30))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvPropertyLessThan() {
        final List<Vertex> result = tstvG.V().out().has(NAME, P.lt(OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByLabelAndPropertyMoreThan() {
        final List<Vertex> result = g.V().out().has(PERSON, AGE, P.gt(30))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvPropertyMoreThan() {
        final List<Vertex> result = tstvG.V().out().has(NAME, P.gt(TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(OTHER_TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByLabelAndPropertyWithin() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, P.within(VADAS.getName(), JOSH.getName()))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByLabelAndTstvPropertyWithin() {
        final List<Vertex> result = tstvG.V().out()
                .has(TSTV, NAME, P.within(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(OTHER_TSTV_PROPERTY_STRING, TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByPropertyInside() {
        final List<Object> result = g.V().out().has(PERSON, AGE, P.inside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(VADAS.getAge());
    }

    @Test
    void shouldFilterVerticesByPropertyOutside() {
        final List<Object> result = g.V().out().has(PERSON, AGE, P.outside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(JOSH.getAge());
    }

    @Test
    void shouldFilterVerticesByPropertyWithin() {
        final List<Vertex> result = g.V().out().has(NAME, P.within(JOSH.getName(), RIPPLE.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), RIPPLE.getId());
    }

    @Test
    void shouldFilterVerticesByTstvPropertyWithin() {
        final List<Vertex> result = tstvG.V().out()
                .has(NAME, P.within(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result)
                .extracting(r -> r.value(NAME))
                .containsExactlyInAnyOrder(OTHER_TSTV_PROPERTY_STRING, TSTV_PROPERTY_STRING);
    }

    @Test
    void shouldFilterVerticesByPropertyWithout() {
        final List<Vertex> result = g.V().out().has(NAME, P.without(JOSH.getName(), LOP.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), RIPPLE.getId());
    }

    @Test
    void shouldFilterVerticesByTstvPropertyWithout() {
        final List<Vertex> result = tstvG.V().out()
                .has(NAME, P.without(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING))
                .toList();

        assertThat(result).isEmpty();
    }

    @Test
    void shouldFilterVerticesByPropertyNotWithin() {
        final List<Vertex> result = g.V().out().has(NAME, P.not(P.within(JOSH.getName(), LOP.getName()))).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), RIPPLE.getId());
    }

    @Test
    void shouldFilterVerticesByTstvPropertyNotWithin() {
        final List<Vertex> result = tstvG.V().out()
                .has(NAME, P.not(P.within(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING)))
                .toList();

        assertThat(result).isEmpty();
    }

  @Test
     void shouldFilterVerticesByPropertyNot() {
         final List<Vertex> result = g.V().out().hasNot(AGE).toList();

         assertThat(result)
                 .extracting(r -> r.id())
                 .containsExactlyInAnyOrder(RIPPLE.getId(), LOP.getId());
     }

    @Test
    void shouldFilterVerticesByPropertyStartingWith() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.startingWith("j")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotStartingWith() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.notStartingWith("j")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyEndingWith() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.endingWith("s")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotEndingWith() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.notEndingWith("s")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyContaining() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.containing("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotContaining() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.notContaining("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyRegex() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.regex("j.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId());
    }

    @Test
    void shouldFilterVerticesByPropertyNotRegex() {
        final List<Vertex> result = g.V().out().has(PERSON, NAME, TextP.notRegex("j.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId());
    }

    @Test
    void shouldHandleNulls() {
        final List<Long> result = g.V().out().has(AGE, P.within(27, null)).count().toList();
        assertThat(result).containsExactly(1L);
    }

    @Test
    void shouldFilterVertexById() {
        final List<Vertex> result = g.V().out().hasId(MARKO.getId(), JOSH.getId()).toList();
        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId());
    }

    @Test
    void shouldFilterVertexByIdMissing() {
        final List<Vertex> result = g.V().out().hasId(new String[0]).toList();
        assertThat(result).isEmpty();
    }

}
