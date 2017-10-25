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
package uk.gov.gchq.gaffer.operation.impl;

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.ArrayList;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

public class ExtractItemsTest extends OperationTest {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Iterable<Iterable<? extends Object>> input = new ArrayList<>();
        final ExtractItems operation = new ExtractItems.Builder()
                .input(input)
                .selection(1)
                .build();

        // Then
        assertThat(operation.getInput(), is(notNullValue()));
        assertEquals(1, operation.getSelection());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Iterable<Iterable<? extends Object>> input = new ArrayList<>();
        final ExtractItems operation = new ExtractItems.Builder()
                .input(input)
                .selection(2)
                .build();

        // When
        final ExtractItems clone = operation.shallowClone();

        // Then
        assertNotSame(operation, clone);
        assertEquals(input, clone.getInput());
        assertEquals(2, clone.getSelection());
    }

    @Override
    protected Object getTestObject() {
        return new ExtractItems();
    }
}
