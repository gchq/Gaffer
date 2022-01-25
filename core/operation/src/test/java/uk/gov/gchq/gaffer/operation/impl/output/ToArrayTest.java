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

package uk.gov.gchq.gaffer.operation.impl.output;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;

import com.google.common.collect.Lists;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ToArrayTest extends OperationTest<ToArray> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToArray<String> toArray = new ToArray.Builder<String>().input("1", "2").build();

        // Then
        final List<String> input = Lists.newArrayList(toArray.getInput());
        assertThat(input)
                .hasSize(2)
                .containsOnly("1", "2");
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String input = "input";
        final ToArray toArray = new ToArray.Builder<>()
                .input(input)
                .build();

        //When
        final ToArray clone = toArray.shallowClone();

        // Then
        assertNotSame(toArray, clone);
        assertThat(clone.getInput().iterator().next()).isEqualTo(input);
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Object.class, outputClass);
    }

    @Override
    protected ToArray getTestObject() {
        return new ToArray();
    }
}
