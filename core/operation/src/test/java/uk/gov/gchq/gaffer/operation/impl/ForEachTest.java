/*
 * Copyright 2018 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

public class ForEachTest extends OperationTest<ForEach> {

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        List inputIterable = Arrays.asList("one", "two");

        final ForEach operation = new ForEach.Builder<>()
                .input(inputIterable)
                .operation(GetElements.class)
                .build();

        // Then
        assertThat(operation.getInput(), is(notNullValue()));
        assertEquals(inputIterable, operation.getInput());
        assertEquals(operation.getOperation(), GetElements.class);
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        List inputIterable = Arrays.asList("one", "two");
        final ForEach operation = new ForEach.Builder<>()
                .input(inputIterable)
                .build();

        // When
        final ForEach clone = operation.shallowClone();

        // Then
        assertNotSame(operation, clone);
        assertEquals(inputIterable, clone.getInput().iterator().next());
    }

    @Override
    protected ForEach getTestObject() {
        return new ForEach();
    }
}
