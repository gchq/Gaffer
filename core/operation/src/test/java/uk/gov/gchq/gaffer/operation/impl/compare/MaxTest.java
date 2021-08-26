/*
 * Copyright 2016-2021 Crown Copyright
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
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class MaxTest extends OperationTest<Max> {

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("comparators");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Max max = new Max.Builder().input(new Entity.Builder()
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
        assertThat(max.getInput())
                .hasSize(2);
        List properties = Streams.toStream(max.getInput()).map(e -> e.getProperty("property")).collect(toList());
        assertThat(properties).containsOnly(1, 2);
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Entity input = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("property", 1)
                .build();
        final ElementPropertyComparator comparator = new ElementPropertyComparator();
        final Max max = new Max.Builder()
                .input(input)
                .comparators(comparator)
                .build();

        // When
        Max clone = max.shallowClone();

        // Then
        assertNotSame(max, clone);
        assertThat(clone.getInput().iterator().next()).isEqualTo(input);
        assertThat(clone.getComparators().iterator().next()).isEqualTo(comparator);
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Element.class, outputClass);
    }

    @Override
    protected Max getTestObject() {
        return new Max();
    }
}
