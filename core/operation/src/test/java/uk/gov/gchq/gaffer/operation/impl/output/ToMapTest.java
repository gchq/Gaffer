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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.MapGenerator;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ToMapTest extends OperationTest<ToMap> {

    @Override
    protected Set<String> getRequiredFields() {
        return Collections.singleton("elementGenerator");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToMap toMap = new ToMap.Builder()
                .input(new Entity(TestGroups.ENTITY), new Entity(TestGroups.ENTITY_2))
                .generator(new MapGenerator())
                .build();

        // Then
        assertThat(toMap.getInput())
                .hasSize(2);
        assertThat(toMap.getElementGenerator()).isNotNull();
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Entity input = new Entity(TestGroups.ENTITY);
        final MapGenerator generator = new MapGenerator();
        final ToMap toMap = new ToMap.Builder()
                .input(input)
                .generator(generator)
                .build();

        // When
        ToMap clone = toMap.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(toMap);
        assertThat(clone.getInput()).isEqualTo(Arrays.asList(input));
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertThat(outputClass).isEqualTo(Iterable.class);
    }

    @Override
    protected ToMap getTestObject() {
        return new ToMap();
    }
}
