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
import uk.gov.gchq.gaffer.operation.impl.compare.Sort.Builder;

import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SortTest extends OperationTest<Sort> {

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("comparators");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Sort sort = new Builder().input(new Entity.Builder()
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
        assertThat(sort.getInput(), is(notNullValue()));
        assertThat(sort.getInput(), iterableWithSize(2));
        assertThat(Streams.toStream(sort.getInput())
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
        final Boolean deDuplicate = false;
        final int resultLimit = 5;
        final Sort sort = new Sort.Builder()
                .input(input)
                .comparators(comparator)
                .resultLimit(resultLimit)
                .deduplicate(deDuplicate)
                .build();

        // When
        Sort clone = sort.shallowClone();

        // Then
        assertNotSame(sort, clone);
        assertEquals(input, clone.getInput().iterator().next());
        assertEquals(comparator, clone.getComparators().iterator().next());
        assertEquals(deDuplicate, clone.isDeduplicate());
        assertTrue(clone.getResultLimit().equals(resultLimit));
    }

    @Override
    protected Sort getTestObject() {
        return new Sort();
    }
}