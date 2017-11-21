/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.impl.compare;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.compare.Min.Builder;

import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

public class MinTest extends OperationTest<Min> {

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("comparators");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Min min = new Builder().input(new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("property", 1)
                .build(), new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("property", 2)
                .build()).comparators(new ElementPropertyComparator() {
            @Override
            public int compare(final Element e1, final Element e2) {
                return 0;
            }
        }).build();

        // Then
        assertThat(min.getInput(), is(notNullValue()));
        assertThat(min.getInput(), iterableWithSize(2));
        assertThat(Streams.toStream(min.getInput())
                .map(e -> e.getProperty("property"))
                .collect(toList()), containsInAnyOrder(1, 2));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Entity input = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("property", 1)
                .build();
        final ElementPropertyComparator comparator = new ElementPropertyComparator();
        final Min min = new Min.Builder()
                .input(input)
                .comparators(comparator)
                .build();

        // When
        Min clone = min.shallowClone();

        // Then
        assertNotSame(min, clone);
        assertEquals(input, clone.getInput().iterator().next());
        assertEquals(comparator, clone.getComparators().iterator().next());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Element.class, outputClass);
    }

    @Override
    protected Min getTestObject() {
        return new Min();
    }
}