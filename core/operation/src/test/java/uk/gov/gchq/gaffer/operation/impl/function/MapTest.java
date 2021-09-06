/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.impl.function;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.Map;

import java.util.Arrays;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class MapTest extends OperationTest<Map> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Map<Integer, Long> map = new Map.Builder<Integer>()
                .input(3)
                .first(Object::toString)
                .then(Integer::parseInt)
                .then(i -> (long) i)
                .build();

        // Then
        assertNotNull(map.getInput());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Iterable<Integer> input = Arrays.asList(1, 2, 3);

        final Map<Iterable<Integer>, String> map = new Map.Builder<Iterable<Integer>>()
                .input(input)
                .first(Object::toString)
                .build();

        // When
        final Map<Iterable<Integer>, String> clone = map.shallowClone();

        // Then
        assertNotSame(map, clone);
        assertThat(clone.getInput().iterator().next()).isEqualTo(new Integer(1));
    }

    @Override
    protected Map getTestObject() {
        return new Map();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("functions");
    }
}
