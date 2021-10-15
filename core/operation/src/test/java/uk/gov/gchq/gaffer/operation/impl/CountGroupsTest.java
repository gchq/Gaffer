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

package uk.gov.gchq.gaffer.operation.impl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class CountGroupsTest extends OperationTest<CountGroups> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final CountGroups countGroups = new CountGroups.Builder()
                .input(new Entity(TestGroups.ENTITY), new Entity(TestGroups.ENTITY_2))
                .limit(1)
                .build();

        // Then
        Assertions.<Element>assertThat(countGroups.getInput())
                .hasSize(2)
                .containsOnly(new Entity(TestGroups.ENTITY), new Entity(TestGroups.ENTITY_2));
        assertThat(countGroups.getLimit()).isEqualTo(1);
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final int limit = 3;
        final Entity input = new Entity(TestGroups.ENTITY);
        final CountGroups countGroups = new CountGroups.Builder()
                .input(input)
                .limit(limit)
                .build();

        // When
        CountGroups clone = countGroups.shallowClone();

        // Then
        assertNotSame(countGroups, clone);
        assertEquals(limit, (int) clone.getLimit());
        assertThat(clone.getInput().iterator().next()).isEqualTo(input);
    }

    @Test
    public void shouldGetOutputClass() {
        final Class<?> outputClass = getTestObject().getOutputClass();

        assertEquals(GroupCounts.class, outputClass);
    }

    @Override
    protected CountGroups getTestObject() {
        return new CountGroups();
    }
}
