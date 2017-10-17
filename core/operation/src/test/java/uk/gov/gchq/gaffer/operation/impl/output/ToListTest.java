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

package uk.gov.gchq.gaffer.operation.impl.output;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;


public class ToListTest extends OperationTest<ToList> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToList<String> toList = new ToList.Builder<String>().input("1", "2").build();

        // Then
        assertThat(toList.getInput(), is(notNullValue()));
        assertThat(toList.getInput(), iterableWithSize(2));
        assertThat(toList.getInput(), containsInAnyOrder("1", "2"));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String input = "1";
        final ToList toList = new ToList.Builder<>()
                .input(input)
                .build();

        // When
        final ToList clone = toList.shallowClone();

        // Then
        assertNotSame(toList, clone);
        assertEquals(input, clone.getInput().iterator().next());
    }

    @Override
    protected ToList getTestObject() {
        return new ToList();
    }
}