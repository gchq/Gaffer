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

import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.AGE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MODERN_CONFIGURATION;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.VADAS;

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
    public void shouldFilterByLabel() {
        final List<Vertex> result = g.V().hasLabel(PERSON).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId(), JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterByLabelsOnly() {
        final List<Vertex> result = g.V().hasLabel(PERSON, NAME, MARKO.getName()).toList();

        // Correct behaviour is to treat all args as labels when using hasLabel
        // All 'person' vertices returned
        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId(), JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterByLabelAndProperty() {
        final List<Vertex> result = g.V().has(PERSON, NAME, MARKO.getName()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterByLabelAndPropertyWithin() {
        final List<Vertex> result = g.V().hasLabel(PERSON).out().has(NAME, P.within(VADAS.getName(), JOSH.getName()))
                .toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId());
    }

    @Test
    public void shouldFilterByPropertyInside() {
        final List<Object> result = g.V().has(AGE, P.inside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(MARKO.getAge(), VADAS.getAge());
    }

    @Test
    public void shouldFilterByPropertyOutside() {
        final List<Object> result = g.V().has(AGE, P.outside(20, 30)).values(AGE).toList();

        assertThat(result)
                .extracting(r -> (Integer) r)
                .containsExactlyInAnyOrder(JOSH.getAge(), PETER.getAge());
    }

    @Test
    public void shouldFilterByPropertyWithin() {
        final List<Vertex> result = g.V().has(NAME, P.within(JOSH.getName(), MARKO.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), MARKO.getId());
    }

    @Test
    public void shouldFilterByPropertyWithout() {
        final List<Vertex> result = g.V().has(NAME, P.without(JOSH.getName(), MARKO.getName())).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    public void shouldFilterByPropertyNotWithin() {
        final List<Vertex> result = g.V().has(NAME, P.not(P.within(JOSH.getName(), MARKO.getName()))).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), PETER.getId(), RIPPLE.getId(), LOP.getId());
    }

    @Test
    public void shouldFilterByPropertyNot() {
        final List<Vertex> result = g.V().hasNot(AGE).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(RIPPLE.getId(), LOP.getId());
    }

    @Test
    public void shouldFilterByPropertyStartingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.startingWith("m")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterByPropertyNotStartingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notStartingWith("m")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterByPropertyEndingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.endingWith("o")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterByPropertyNotEndingWith() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notEndingWith("o")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(VADAS.getId(), JOSH.getId(), PETER.getId());
    }


    @Test
    public void shouldFilterByPropertyContaining() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.containing("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), VADAS.getId());
    }

    @Test
    public void shouldFilterByPropertyNotContaining() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notContaining("a")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), PETER.getId());
    }

    @Test
    public void shouldFilterByPropertyRegex() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.regex("m.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId());
    }

    @Test
    public void shouldFilterByPropertyNotRegex() {
        final List<Vertex> result = g.V().has(PERSON, NAME, TextP.notRegex("m.*")).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(JOSH.getId(), VADAS.getId(), PETER.getId());
    }
}
