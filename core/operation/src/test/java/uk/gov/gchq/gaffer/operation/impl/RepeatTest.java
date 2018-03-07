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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RepeatTest extends OperationTest<Repeat> {

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Repeat repeat = new Repeat.Builder()
                .input(new EntitySeed("1"))
                .operation(new GetAdjacentIds())
                .times(2)
                .build();

        // Then
        assertThat(repeat.getInput(), is(notNullValue()));
        assertEquals(2, repeat.getTimes());
        assertTrue(repeat.getOperation() instanceof GetAdjacentIds);
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final EntitySeed input = new EntitySeed("A");
        final int times = 4;
        final Operation delegate = new GetAdjacentIds();

        final Repeat repeat = new Repeat.Builder()
                .input(input)
                .operation(delegate)
                .times(times)
                .build();

        // When
        final Repeat clone = repeat.shallowClone();

        // Then
        assertNotSame(repeat, clone);
        assertEquals(input, clone.getInput());
        assertEquals(times, clone.getTimes());
    }

    @Override
    protected Repeat getTestObject() {
        return new Repeat.Builder()
                .input(new EntitySeed("A"))
                .operation(new GetAdjacentIds())
                .times(3)
                .build();
    }
}
