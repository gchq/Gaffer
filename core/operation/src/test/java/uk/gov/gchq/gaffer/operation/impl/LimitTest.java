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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class LimitTest extends OperationTest<Limit> {

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("resultLimit");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Limit<String> limit = new Limit.Builder<String>().input("1", "2").resultLimit(1).build();

        // Then
        assertThat(limit.getInput())
                .hasSize(2);
        assertThat(limit.getResultLimit()).isEqualTo(1);
        assertThat(limit.getInput()).containsOnly("1", "2");
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String input = "1";
        final int resultLimit = 4;
        final Limit limit = new Limit.Builder<>()
                .input(input)
                .resultLimit(resultLimit)
                .truncate(false)
                .build();

        // When
        final Limit clone = limit.shallowClone();

        // Then
        assertNotSame(limit, clone);
        assertThat(clone.getInput().iterator().next()).isEqualTo(input);
        assertEquals(resultLimit, (int) clone.getResultLimit());
        assertFalse(clone.getTruncate());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected Limit getTestObject() {
        return new Limit();
    }
}
