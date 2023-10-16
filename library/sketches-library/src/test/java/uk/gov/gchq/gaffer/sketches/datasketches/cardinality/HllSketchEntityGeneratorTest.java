/*
 * Copyright 2021-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality;

import org.apache.datasketches.hll.HllSketch;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HllSketchEntityGeneratorTest {
    private static final String DEFAULT_ENTITY_GROUP = "Cardinality";
    private static final String DEFAULT_PROPERTY_NAME = "cardinality";
    private static final String A = "A";
    private static final String B = "B";
    private static final String PROP1 = "prop1";
    private static final String VALUE1 = "value1";

    @Test
    public void shouldCreateSimpleEntities() {
        // Given
        final HllSketchEntityGenerator hllSketchEntityGenerator = new HllSketchEntityGenerator();
        final Edge edge = new Edge.Builder()
                .group(TestGroups.ENTITY)
                .source(A)
                .dest(B)
                .build();
        final List<? extends Element> edges = Arrays.asList(edge);

        // When
        final Iterable<? extends Element> elements = hllSketchEntityGenerator.apply(edges);

        // Then
        final Iterator<? extends Element> elementIterator = elements.iterator();
        final Edge edgeResult = (Edge) elementIterator.next();
        final Entity entityResultA = (Entity) elementIterator.next();
        final Entity entityResultB = (Entity) elementIterator.next();

        assertThat(elementIterator.hasNext()).isFalse();
        assertThat(edgeResult).isEqualTo(edge);

        assertThat(entityResultA.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultA.getVertex()).isEqualTo(A);
        final HllSketch entityCardinalityA = (HllSketch) entityResultA.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityA.getEstimate()).isEqualTo(1);

        assertThat(entityResultB.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultB.getVertex()).isEqualTo(B);
        final HllSketch entityCardinalityB = (HllSketch) entityResultB.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityB.getEstimate()).isEqualTo(1);
    }

    @Test
    public void shouldCreateSimpleEntitiesWithProperties() {
        // Given
        final HllSketchEntityGenerator hllSketchEntityGenerator = new HllSketchEntityGenerator();
        hllSketchEntityGenerator.propertyToCopy(PROP1);
        final Edge edge = new Edge.Builder()
                .group(TestGroups.ENTITY)
                .source(A)
                .dest(B)
                .property(PROP1, VALUE1)
                .build();
        final List<? extends Element> edges = Arrays.asList(edge);

        // When
        final Iterable<? extends Element> elements = hllSketchEntityGenerator.apply(edges);

        // Then
        final Iterator<? extends Element> elementIterator = elements.iterator();
        final Edge edgeResult = (Edge) elementIterator.next();
        final Entity entityResultA = (Entity) elementIterator.next();
        final Entity entityResultB = (Entity) elementIterator.next();

        assertThat(elementIterator.hasNext()).isFalse();
        assertThat(edgeResult).isEqualTo(edge);

        assertThat(entityResultA.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultA.getVertex()).isEqualTo(A);
        final HllSketch entityCardinalityA = (HllSketch) entityResultA.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityA.getEstimate()).isEqualTo(1);
        assertThat(entityResultA.getProperty(PROP1)).isEqualTo(VALUE1);

        assertThat(entityResultB.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultB.getVertex()).isEqualTo(B);
        final HllSketch entityCardinalityB = (HllSketch) entityResultB.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityB.getEstimate()).isEqualTo(1);
        assertThat(entityResultB.getProperty(PROP1)).isEqualTo(VALUE1);
    }

}
