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
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IfTest extends OperationTest {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final If filter = getTestObject();

        // Then
        assertThat(filter.getInput(), is(notNullValue()));
        assertTrue(filter.getCondition());
        assertTrue(filter.getThen() instanceof GetElements);
        assertTrue(filter.getOtherwise() instanceof GetAllElements);
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Object input = "testInput";
        final If filter = new If.Builder()
                .input(input)
                .condition(true)
                .then(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .otherwise(new GetAllElements())
                .build();

        // When
        final If clone = filter.shallowClone();

        // Then
        assertNotSame(filter, clone);
        assertEquals(input, clone.getInput());
    }

    @Override
    protected If getTestObject() {
        return new If.Builder()
                .input("testInput")
                .condition(true)
                .then(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .otherwise(new GetAllElements())
                .build();
    }
}
