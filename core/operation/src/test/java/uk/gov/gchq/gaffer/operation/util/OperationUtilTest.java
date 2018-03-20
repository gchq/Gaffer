/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.util;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OperationUtilTest {

    @Test
    public void shouldReturnNullIfConvertObjectArrayToElementIdsWithNullInput() {
        // Given
        final Object[] input = null;

        // When
        final Iterable<? extends ElementId> output = OperationUtil.toElementIds(input);

        // Then
        assertNull(output);
    }

    @Test
    public void shouldConvertObjectArrayToElementIds() {
        // Given
        final Object[] input = {
                1,
                "2",
                new EntitySeed("3"),
                new Entity("group", "4"),
                new EdgeSeed("5", 6),
                new Edge("group", 7L, 8, true),
                null
        };

        // When
        final Iterable<? extends ElementId> output = OperationUtil.toElementIds(input);

        // Then
        assertEquals(
                Lists.newArrayList(
                        new EntitySeed(1),
                        new EntitySeed("2"),
                        new EntitySeed("3"),
                        new Entity("group", "4"),
                        new EdgeSeed("5", 6),
                        new Edge("group", 7L, 8, true),
                        null
                ),
                Lists.newArrayList(output)
        );
    }

    @Test
    public void shouldConvertIterableToElementIds() {
        // Given
        final Iterable<Object> input = Arrays.asList(
                1,
                "2",
                new EntitySeed("3"),
                new Entity("group", "4"),
                new EdgeSeed("5", 6),
                new Edge("group", 7L, 8, true),
                null
        );

        // When
        final Iterable<? extends ElementId> output = OperationUtil.toElementIds(input);

        // Then
        assertEquals(
                Lists.newArrayList(
                        new EntitySeed(1),
                        new EntitySeed("2"),
                        new EntitySeed("3"),
                        new Entity("group", "4"),
                        new EdgeSeed("5", 6),
                        new Edge("group", 7L, 8, true),
                        null
                ),
                Lists.newArrayList(output)
        );
    }

    @Test
    public void shouldReturnNullIfConvertFromElementIdsWithNullInput() {
        // Given
        final Iterable<? extends EntityId> input = null;

        // When
        final Iterable<?> output = OperationUtil.fromElementIds(input);

        // Then
        assertNull(output);
    }

    @Test
    public void shouldConvertFromElementIds() {
        // Given
        final Iterable<ElementId> input = Arrays.asList(
                new EntitySeed(1),
                new EntitySeed("2"),
                new EntitySeed("3"),
                new Entity("group", "4"),
                new EdgeSeed("5", 6),
                new Edge("group", 7L, 8, true),
                null
        );

        // When
        final Iterable<?> output = OperationUtil.fromElementIds(input);

        // Then
        assertEquals(
                Lists.newArrayList(
                        1,
                        "2",
                        "3",
                        "4",
                        new EdgeSeed("5", 6),
                        new Edge("group", 7L, 8, true),
                        null
                ),
                Lists.newArrayList(output)
        );
    }

    @Test
    public void shouldReturnNullIfConvertObjectArrayToEntityIdsWithNullInput() {
        // Given
        final Object[] input = null;

        // When
        final Iterable<? extends EntityId> output = OperationUtil.toEntityIds(input);

        // Then
        assertNull(output);
    }

    @Test
    public void shouldConvertObjectArrayToEntityIds() {
        // Given
        final Object[] input = {
                1,
                "2",
                new EntitySeed("3"),
                new Entity("group", "4"),
                null
        };

        // When
        final Iterable<? extends EntityId> output = OperationUtil.toEntityIds(input);

        // Then
        assertEquals(
                Lists.newArrayList(
                        new EntitySeed(1),
                        new EntitySeed("2"),
                        new EntitySeed("3"),
                        new Entity("group", "4"),
                        null
                ),
                Lists.newArrayList(output)
        );
    }

    @Test
    public void shouldConvertIterableToEntityIds() {
        // Given
        final Iterable<Object> input = Arrays.asList(
                1,
                "2",
                new EntitySeed("3"),
                new Entity("group", "4"),
                null
        );

        // When
        final Iterable<? extends ElementId> output = OperationUtil.toEntityIds(input);

        // Then
        assertEquals(
                Lists.newArrayList(
                        new EntitySeed(1),
                        new EntitySeed("2"),
                        new EntitySeed("3"),
                        new Entity("group", "4"),
                        null
                ),
                Lists.newArrayList(output)
        );
    }

    @Test
    public void shouldReturnNullIfConvertFromEntityIdsWithNullInput() {
        // Given
        final Iterable<? extends EntityId> input = null;

        // When
        final Iterable<?> output = OperationUtil.fromEntityIds(input);

        // Then
        assertNull(output);
    }

    @Test
    public void shouldConvertFromEntityIds() {
        // Given
        final Iterable<ElementId> input = Arrays.asList(
                new EntitySeed(1),
                new EntitySeed("2"),
                new EntitySeed("3"),
                new Entity("group", "4"),
                null
        );

        // When
        final Iterable<?> output = OperationUtil.fromElementIds(input);

        // Then
        assertEquals(
                Lists.newArrayList(
                        1,
                        "2",
                        "3",
                        "4",
                        null
                ),
                Lists.newArrayList(output)
        );
    }
}
