/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HyperLogLogPlusEntityGeneratorTest {
    private static final String DEFAULT_ENTITY_GROUP = "Cardinality";
    private static final String DEFAULT_PROPERTY_NAME = "cardinality";
    private static final String A = "A";
    private static final String B = "B";
    private static final String PROP1 = "prop1";
    private static final String VALUE1 = "value1";

    @Test
    public void shouldCreateSimpleEntities() {
        //Given
        HyperLogLogPlusEntityGenerator hyperLogLogPlusEntityGenerator = new HyperLogLogPlusEntityGenerator();
        Edge edge = new Edge.Builder()
                .group(TestGroups.ENTITY)
                .source(A)
                .dest(B)
                .build();
        List<? extends Element> edges = Arrays.asList(edge);

        //When
        Iterable<? extends Element> elements = hyperLogLogPlusEntityGenerator.apply(edges);

        //Then
        Iterator<? extends Element> elementIterator = elements.iterator();
        Edge edgeResult = (Edge) elementIterator.next();
        Entity entityResultA = (Entity) elementIterator.next();
        Entity entityResultB = (Entity) elementIterator.next();

        assertThat(elementIterator.hasNext()).isFalse();
        assertThat(edgeResult).isEqualTo(edge);

        assertThat(entityResultA.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultA.getVertex()).isEqualTo(A);
        HyperLogLogPlus entityCardinalityA = (HyperLogLogPlus) entityResultA.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityA.cardinality()).isEqualTo(1);

        assertThat(entityResultB.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultB.getVertex()).isEqualTo(B);
        HyperLogLogPlus entityCardinalityB = (HyperLogLogPlus) entityResultB.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityB.cardinality()).isEqualTo(1);
    }

    @Test
    public void shouldCreateSimpleEntitiesWithProperties() {
        //Given
        HyperLogLogPlusEntityGenerator hyperLogLogPlusEntityGenerator = new HyperLogLogPlusEntityGenerator();
        hyperLogLogPlusEntityGenerator.propertyToCopy(PROP1);
        Edge edge = new Edge.Builder()
                .group(TestGroups.ENTITY)
                .source(A)
                .dest(B)
                .property(PROP1, VALUE1)
                .build();
        List<? extends Element> edges = Arrays.asList(edge);

        //When
        Iterable<? extends Element> elements = hyperLogLogPlusEntityGenerator.apply(edges);

        //Then
        Iterator<? extends Element> elementIterator = elements.iterator();
        Edge edgeResult = (Edge) elementIterator.next();
        Entity entityResultA = (Entity) elementIterator.next();
        Entity entityResultB = (Entity) elementIterator.next();

        assertThat(elementIterator.hasNext()).isFalse();
        assertThat(edgeResult).isEqualTo(edge);

        assertThat(entityResultA.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultA.getVertex()).isEqualTo(A);
        HyperLogLogPlus entityCardinalityA = (HyperLogLogPlus) entityResultA.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityA.cardinality()).isEqualTo(1);
        assertThat(entityResultA.getProperty(PROP1)).isEqualTo(VALUE1);

        assertThat(entityResultB.getGroup()).isEqualTo(DEFAULT_ENTITY_GROUP);
        assertThat(entityResultB.getVertex()).isEqualTo(B);
        HyperLogLogPlus entityCardinalityB = (HyperLogLogPlus) entityResultB.getProperty(DEFAULT_PROPERTY_NAME);
        assertThat(entityCardinalityB.cardinality()).isEqualTo(1);
        assertThat(entityResultB.getProperty(PROP1)).isEqualTo(VALUE1);
    }

}
