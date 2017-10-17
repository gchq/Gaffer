/*
 * Copyright 2016 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;


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
        assertThat(countGroups.getInput(), is(notNullValue()));
        assertThat(countGroups.getInput(), iterableWithSize(2));
        assertThat(countGroups.getLimit(), is(1));
        assertThat(countGroups.getInput(), containsInAnyOrder(new Entity(TestGroups.ENTITY), new Entity(TestGroups.ENTITY_2)));
    }

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
        assertEquals(input, clone.getInput().iterator().next());
    }

    @Override
    protected CountGroups getTestObject() {
        return new CountGroups();
    }
}