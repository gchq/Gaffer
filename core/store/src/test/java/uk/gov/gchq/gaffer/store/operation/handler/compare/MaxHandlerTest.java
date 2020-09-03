/*
 * Copyright 2017-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.compare;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;

import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MaxHandlerTest {

    @Test
    public void shouldFindMaxBasedOnProperty() throws OperationException {
        // Given
        final Entity entity1 = makeEntity("property", 1);
        final Entity entity2 = makeEntity("property", 2);
        final Entity entity3 = makeEntity("property", 3);
        final Entity entity4 = makeEntity("property", 1);

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Max max = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(TestGroups.ENTITY)
                        .property("property")
                        .build())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertTrue(result instanceof Entity);
        assertEquals(3, result.getProperty("property"));
    }

    @Test
    public void shouldFindMaxBasedOnMultipleProperties() throws OperationException {
        // Given
        final Entity entity1 = makeEntity(1, 1);
        final Entity entity2 = makeEntity(1, 2);
        final Entity entity3 = makeEntity(2, 2);
        final Entity entity4 = makeEntity(2, 1);

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Max max = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                                .groups(TestGroups.ENTITY)
                                .property("property1")
                                .build(),
                        new ElementPropertyComparator.Builder()
                                .groups(TestGroups.ENTITY)
                                .property("property2")
                                .build())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertSame(entity3, result);
    }

    @Test
    public void shouldFindMaxBasedOnPropertyWithMissingProperty() throws OperationException {
        // Given
        final Entity entity1 = makeEntity("property1", 1);
        final Entity entity2 = makeEntity("property1", 2);
        final Entity entity3 = makeEntity("property1", 3);
        final Entity entity4 = makeEntity("property2", 1);
        final Entity entity5 = makeEntity("property2", 2);

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4, entity5);

        final Max max1 = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(TestGroups.ENTITY)
                        .property("property1")
                        .build())
                .build();

        final Max max2 = new Max.Builder().input(input)
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(TestGroups.ENTITY)
                        .property("property2")
                        .build())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result1 = handler.doOperation(max1, null, null);
        final Element result2 = handler.doOperation(max2, null, null);

        // Then
        assertTrue(result1 instanceof Entity);
        assertTrue(result2 instanceof Entity);
        assertEquals(3, result1.getProperty("property1"));
        assertEquals(2, result2.getProperty("property2"));
    }

    @Test
    public void shouldFindMaxBasedOnElement() throws OperationException {
        // Given
        final Entity entity1 = makeEntity(1, 1);
        final Entity entity2 = makeEntity(2, 2);
        final Entity entity3 = makeEntity(3, 3);
        final Entity entity4 = makeEntity(4, 4);

        final List<Entity> input = Lists.newArrayList(entity1, entity2, entity3, entity4);

        final Max max = new Max.Builder().input(input)
                .comparators(new SimpleElementComparator())
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertTrue(result instanceof Entity);
        assertEquals(4, result.getProperty("property1"));
        assertEquals(4, result.getProperty("property2"));
    }

    @Test
    public void shouldReturnNullIfOperationInputIsNull() throws OperationException {
        // Given
        final Max max = new Max.Builder().build();
        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldReturnNullIfBothComparatorsAreNull() throws OperationException {
        // Given
        final List<Entity> input = Lists.newArrayList();
        final Max max = new Max.Builder().input(input)
                .build();

        final MaxHandler handler = new MaxHandler();

        // When
        final Element result = handler.doOperation(max, null, null);

        // Then
        assertNull(result);
    }

    private Entity makeEntity(final String propertyName1, final int property1) {
        return new Entity.Builder().group(TestGroups.ENTITY)
                .property(propertyName1, property1)
                .build();
    }

    private Entity makeEntity(final int property1, final int property2) {
        return new Entity.Builder().group(TestGroups.ENTITY)
                .property("property1", property1)
                .property("property2", property2)
                .build();
    }

    private static class SimpleElementComparator implements Comparator<Element> {
        @Override
        public int compare(final Element obj1, final Element obj2) {
            final int v1 = (int) obj1.getProperty("property1") * (int) obj1.getProperty("property2");
            final int v2 = (int) obj2.getProperty("property1") * (int) obj2.getProperty("property2");
            return v1 - v2;
        }
    }
}
