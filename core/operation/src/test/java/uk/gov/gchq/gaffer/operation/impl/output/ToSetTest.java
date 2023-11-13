/*
 * Copyright 2016-2023 Crown Copyright
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

import java.util.Arrays;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ToSetTest extends OperationTest<ToSet> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToSet<String> toSet = new ToSet.Builder<String>().input("1", "2").build();

        // Then
        assertThat((Iterable<String>) toSet.getInput())
                .hasSize(2)
                .containsOnly("1", "2");
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String input = "1";
        final ToSet toSet = new ToSet.Builder<>()
                .input(input)
                .build();

        // When
        final ToSet clone = toSet.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(toSet);
        assertThat(clone.getInput()).isEqualTo(Arrays.asList(input));
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertThat(outputClass).isEqualTo(Set.class);
    }

    @Override
    protected ToSet getTestObject() {
        return new ToSet();
    }
}
