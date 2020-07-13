/*
 * Copyright 2018-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ToElementIdTest {
    @Test
    public void shouldReturnNullIfTheInputIsNull() {
        // Given
        final Object input = null;
        final ToElementId function = new ToElementId();

        // When
        final ElementId output = function.apply(input);

        // Then
        assertNull(output);
    }

    @Test
    public void shouldReturnEntitySeedIfInputIsAnObject() {
        // Given
        final Object input = "item";
        final ToElementId function = new ToElementId();

        // When
        final ElementId output = function.apply(input);

        // Then
        assertEquals(new EntitySeed(input), output);
    }

    @Test
    public void shouldReturnOriginalValueIfInputIsAnEntitySeed() {
        // Given
        final EntitySeed input = new EntitySeed("item");
        final ToElementId function = new ToElementId();

        // When
        final ElementId output = function.apply(input);

        // Then
        assertSame(input, output);
    }

    @Test
    public void shouldReturnOriginalValueIfInputIsAnEntity() {
        // Given
        final Entity input = new Entity("group", "item");
        final ToElementId function = new ToElementId();

        // When
        final ElementId output = function.apply(input);

        // Then
        assertSame(input, output);
    }

    @Test
    public void shouldReturnOriginalValueIfInputIsAnEdge() {
        // Given
        final Edge input = new Edge("group");
        final ToElementId function = new ToElementId();

        // When
        final ElementId output = function.apply(input);

        // Then
        assertSame(input, output);
    }
}
